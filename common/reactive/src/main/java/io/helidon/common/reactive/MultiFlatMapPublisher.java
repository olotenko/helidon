package io.helidon.common.reactive;

public class MultiFlatMapPublisher<X> implements Flow.Publisher<X>, Flow.Subscription {
   protected ConcurrentLinkedQueue<InnerSubscriber> readReady = new ConcurrentLinkedQueue<>();
   protected volatile Throwable error;
   protected boolean cancelled;
   protected volatile boolean cancelPending;
   protected boolean completed;
   protected long delivered;
   protected final boolean delayErrors;

   public MultiFlatMapPublisher(Flow.Publisher<Flow.Publisher<X>> source,
                                int maxConcurrency,
                                int prefetch,
                                boolean delayErrors) {
      this.maxConcurrency = maxConcurrency;
      this.prefetch = prefetch;
      this.delayErrors = delayErrors;
   }

   public void subscribe(Flow.Subscriber<X> s) {
      this.downstream = s;
   }

   public void onSubscription(Flow.Subscription sub) {
      this.sub = sub;
      downstream.onSubscription(this);
      if (!cancelled) {
         // assert: there are cases not prescribed by the spec, but which require good hygiene:
         //         if the flow is wired incorrectly, downstream may have been subscribed to
         //         something already; and all they can do, is cancel. In these cases emitting
         //         any signals to downstream would result in completely broken state machine.
         sub.request(maxConcurrency);
      }
   }

   public void onNext(Flow.Publisher<X> p) {
      awaitingTermination.getAndIncrement();
      p.subscribe(new InnerSubscriber());
   }

   public void onError(Throwable th) {
      error = th;
      if (!delayErrors) {
         cancelPending = true;
      }

      complete();
   }

   public void onComplete() {
      complete();
   }

   protected void complete() {
      completed = true;
      maybeDrain();
   }

   public void request(long n) {
      if (n <= 0) {
         if (!completed) {
            onError(new IllegalArgumentException("Expected positive request, found " + n));
         }
         return;
      }

      long r;
      do {
         r = requested.get();
         if (r < 0) {
            return;
         }
      } while(!requested.compareAndSet(r, Long.MAX_VALUE - r <= n? Long.MAX_VALUE: r + n));

      maybeDrain();
   }

   public void cancel() {
      cancelled = true;
      cancelPending = true;
      maybeDrain();
   }

   protected void cleanup() {
      sub.cancel();
      for(InnerSubscriber s: concurrentSubs.keySet()) {
         s.cancel();
      }
      // assert: concurrentSubs concurrent modification will ensure new InnerSubscribers are as good
      //         as after s.cancel()
      concurrentSubs.clear();

      // assert: readReady modification is single-threaded
      // assert: no downstream.onNext is reachable after this readReady.clear()
      //         - it is called only after cancelPending is observed, and other
      //         accesses either ensure they are single-threaded (own readReady),
      //         or do not add to readReady, or will ensure cleanup() is called again
      //         (will ensure drain() is called)
      readReady.clear();
   }

   protected class InnerSubscriber implements Flow.Subscriber<X> {
      protected Flow.Subscription sub;
      protected int consumed;
      // assert: innerQ accesses to head and tail are singlethreaded;
      // assert: put modifies tail, and uses of put ensure the cleanup of the queue
      //         happens eventually, if cancellation has been requested concurrently
      // assert: changes to tail.next become visible to the thread accessing head -
      //         a happens-before edge is established by readReady.put -> readReady.empty
      //         or concurrentSubs.put -> concurrentSubs.keySet
      // assert: changes to tail become visible to the thread accessing tail - on* are serialized
      // assert: empty, clear and poll do not modify tail; any concurrent changes to tail
      //         will be observed: the method that will observe these changes is always invoked
      protected final InnerQueue<X> innerQ = new InnerQueue<>();

      public void cancel() {
         sub.cancel();
         innerQ.clear();
      }

      public void onSubscription(Flow.Subscription sub) {
         this.sub = sub;
         concurrentSubs.put(this, this);
         if (cancelPending) {
            sub.cancel();
            concurrentSubs.remove(this);
            return;
         }
         // assert: the cancellation loop will observe this subscriber
         sub.request(prefetch);
      }

      public void onError(Throwable th) {
         error = th;
         if (!delayErrors) {
            cancelPending = true;
         }

         complete();
      }

      public void onComplete() {
         readReady.put(this);
         complete();
      }

      protected void complete() {
         concurrentSubs.remove(this);
         awaitingTermination.getAndDecrement();
         maybeDrain();
      }

      public void onNext(X item) {
         if (cancelPending) {
            return;
         }

         boolean locked = tryLockDrain();
         // assert: an atomic check for readReady.empty() is a sufficient condition for
         //         FIFO order in the presence of out of bounds synchronization of concurrent
         //         Publishers: if it becomes non-empty after the check, it is evidence
         //         of the absence of such synchronization, in which case the items can be
         //         delivered after this item
         // assert: readReady.empty() implies innerQ.empty()
         if (locked && readReady.empty() && !cancelPending && requested.get() > delivered) {
            consumed();
            downstream.onNext(item);
            delivered++;
            drain();
            return;
         }

         innerQ.putNotSafe(item);
         // assert: drain() observing cancellation may have cleared concurrentSubs and
         //         innerQ before an item has been added to innerQ - need to make sure
         //         innerQ is cleared before the next exit from drain()
         if (cancelPending) {
            // assert: this line is reachable only in the presence of a cleanup() concurrent
            //         with this onNext() - it will call this.cancel()
            // assert: concurrent cleanup() will call innerQ.clear(); we don't need to contend
            //         modifying head - just drop the item that has just been added
            // assert: innerQ will at most reference two Nodes, but no items, after cleanup()
            //         and this onNext complete
            innerQ.clearTailNotSafe();
         } else {
            // assert: if readReady is modified after readReady.clear() in cleanup(), then
            //         the following drain() or maybeDrain() will make sure cleanup() is called
            //         again - the item cannot be consumed out of order, because cleanup() is
            //         called only after cancelPending becomes visible
            readReady.put(this);
         }

         if (locked) {
            drain();
         } else {
            maybeDrain();
         }
      }

      protected void consumed() {
         consumed++;
         if (consumed >= limit) {
            int p = consumed;
            consumed = 0;
            sub.request(p); // assert: if cancelled, no-op
         }
      }

      public X poll() {
         consumed();
         return innerQ.poll();
      }
   }

   // there probably is already a j.u.Queue that fits the bill, but need a guarantee
   // that put and poll are accessed single-threadedly, but not necessarily from the
   // same thread - i.e. put never touches head, and poll never touches tail.
   protected static class InnerQueue<X> {
      protected Node<X> head;
      protected volatile Node<X> tail;

      protected static final VarHandle<Node<X>> vhtail = new VarHandle(InnerQueue.class, "tail");
      protected static final VarHandle<Node<X>> vhnext = new VarHandle(Node.class, "next");

      public InnerQueue() {
         head = tail = new Node();
      }

      public void putNotSafe(X item) {
         // assert: no concurrent calls to put() or putNotSafe()
         Node<X> n = new Node<>();
         n.v = item;
         Node<X> t = tail;
         vhnext.lazySet(t, n);
         vhtail.lazySet(this, n);
      }

      public void clearTailNotSafe() {
         // assert: no concurrent calls to poll() that can access this tail - even if
         //         the queue is not cleared yet
         tail.v = null;
      }

      public void put(X item) {
         Node<X> n = new Node<>();
         n.v = item;
         Node<X> t = tail;
         Node<X> oldt = null;
         do {
            oldt = tail;
            while (t.next != null) {
               t = t.next;
            }
         } while(!vhnext.compareAndSet(t, null, n));
         // assert: ok to update it only sometimes - just subsequent put() will re-scan a bit more,
         //         there is always a future put that succeeds to advance tail a bit
         vhtail.compareAndSet(this, oldt, n);
      }

      public X poll() {
         // assert: no concurrent calls to poll() or clear()
         Node<X> n = head.next;
         if (n == null) {
            return null;
         }
         X v = n.v;
         n.v = null;
         head = n;
         return v;
      }

      public void clear() {
         // assert: no concurrent calls to poll() or clear()
         head = tail;
         head.v = null;
         // assert: if there is a concurrent put, no need to compete to update tail -
         //         in these use cases no successful poll() is executed after clear(),
         //         and concurrent updates of tail will result in a future call to clear()
         vhnext.lazySet(head, null);
      }

      public boolean empty() {
         return head.next == null;
      }
   }

   public static class Node<X> {
      public X v;
      public volatile Node<X> next;
   }

   protected void maybeDrain() {
      if (drainLock.getAndIncrement() != 0) {
         return;
      }
      drain();
   }

   protected boolean tryLockDrain() {
      return drainLock.get() == 0 && drainLock.compareAndSet(0, 1);
   }

   protected void drain() {
      // assert: all the concurrent changes of any variable of any object accessible from here will
      //         result in a change of drainLock
      for(int contenders = 1; contenders != 0; contenders = drainLock.addAndGet(-contenders)) {
         boolean terminate = cancelPending;
         while(!terminate && delivered < requested.get() && !readReady.empty()) {
            X value = readReady.poll().poll();

            if (value == null) {
               sub.request(1L);
            } else {
               downstream.onNext(value);
               delivered++;
            }

            terminate = cancelPending;
         }

         if (terminate) {
            cleanup();
            if (cancelled) {
               continue;
            }
         }

         if (terminate || awaitingTermination.get() == 0 && completed) {
            // assert: terminate == cancelPending is set in two cases:
            //         - cancel() called - in this case this line is not reachable, because cancelled
            //           is observed above
            //         - eager error signalling has been requested - in this case
            //           error is set, and downstream.onError is expected to be called;
            //           after this will behave like cancel() called
            cancelled = true;

            // assert: completed is a guarantee upstream is not producing
            //         any more signals - in particular, awaitingTermination will not increase
            // assert: awaitingTermination == 0 means all InnerSubscribers reached a terminal
            //         state
            // assert: when awaitingTermination is decremented by the last InnerSubscriber,
            //         this line becomes reachable, unless cancelled
            if (error != null) {
               downstream.onError(error);
            } else {
               downstream.onComplete();
            }
         }
      }
   }
}
