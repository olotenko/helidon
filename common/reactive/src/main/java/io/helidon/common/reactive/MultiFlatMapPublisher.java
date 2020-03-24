package io.helidon.common.reactive;

public class MultiFlatMapPublisher<X> implements Flow.Publisher<X>, Flow.Subscription {
   protected ConcurrentLinkedQueue<InnerSubscriber> readReady = new ConcurrentLinkedQueue<>();
   protected Throwable error;
   protected volatile boolean cancelled;
   protected volatile boolean completed;

   public MultiFlatMapPublisher(Flow.Publisher<Flow.Publisher<X>> source,
                                int maxConcurrency,
                                int prefetch) {
      this.maxConcurrency = maxConcurrency;
      this.prefetch = prefetch;
   }

   public void subscribe(Flow.Subscriber<X> s) {
      this.downstream = s;
   }

   public void onSubscription(Flow.Subscription sub) {
      this.sub = sub;
      downstream.onSubscription(this);
   }

   public void onNext(Flow.Publisher<X> p) {
      awaitingTermination.getAndIncrement();
      p.subscribe(new InnerSubscriber());
   }

   public void onError(Throwable th) {
      error = th;
      complete();
   }

   public void onComplete() {
      complete();
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
      sub.cancel();
      maybeDrain();
   }

   protected void cleanup() {
      for(InnerSubscriber s: concurrentSubs.keySet()) {
         s.cancel();
         concurrentSubs.remove(s);
      }

      readReady.clear();
   }

   protected void complete() {
      completed = true;
      maybeDrain();
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
      protected InnerQueue<X> innerQ = new InnerQueue<>();

      public void cancel() {
         sub.cancel();
         innerQ.clear();
      }

      public void onSubscription(Flow.Subscription sub) {
         this.sub = sub;
         concurrentSubs.put(this, this);
         if (cancelled) {
            sub.cancel();
            concurrentSubs.remove(this);
            return;
         }
         // assert: the cancellation loop will observe this subscriber
         sub.request(prefetch);
      }

      public void onError(Throwable th) {
         error = th;
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
         if (cancelled) {
            return;
         }

         boolean locked = tryLockDrain();
         // assert: an atomic check for readReady.empty() is a sufficient condition for
         //         FIFO order in the presence of out of bounds synchronization of concurrent
         //         Publishers: if it becomes non-empty after the check, it is evidence
         //         of the absence of such synchronization, in which case the items can be
         //         delivered after this item
         // assert: readReady.empty() implies innerQ.empty()
         if (locked && readReady.empty() && !cancelled && requested.get() > 0) {
            consumed();
            downstream.onNext(item);
            drain(1);
            return;
         }

         innerQ.put(item);
         // assert: drain() observing cancellation may have cleared concurrentSubs and
         //         innerQ before an item has been added to innerQ - need to make sure
         //         innerQ is cleared before the next exit from drain()
         if (cancelled) {
            concurrentSubs.put(this, this);
         } else {
            readReady.put(this);
         }

         if (locked) {
            drain(0);
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
      Node<X> head;
      Node<X> tail;

      public InnerQueue() {
         head = tail = new Node();
      }

      public void put(X item) {
         Node<X> n = new Node<>();
         n.v = item;
         tail.next = n;
         tail = n;
      }

      public X poll() {
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
         head = tail;
         head.v = null;
      }

      public boolean empty() {
         return head.next == null;
      }
   }

   public static class Node<X> {
      public X v;
      public Node<X> next;
   }

   protected void maybeDrain() {
      if (drainLock.getAndIncrement() != 0) {
         return;
      }
      drain(0);
   }

   protected boolean tryLockDrain() {
      return drainLock.get() == 0 && drainLock.compareAndSet(0, 1);
   }

   protected void drain(long delivered) {
      for(int contenders = 1; contenders != 0; contenders = drainLock.addAndGet(-contenders), delivered = 0) {
         while(!cancelled && delivered < requested.get() && !readReady.empty()) {
            X value = readReady.poll().poll();

            if (value == null) {
               sub.request(1L);
            } else {
               downstream.onNext(value);
               delivered++;
            }
         }

         if (cancelled) {
            cleanup();
            continue;
         }

         long r;
         do {
            r = requested.get();
         } while(r != Long.MAX_VALUE && !requested.compareAndSet(r, r - delivered));

         if (awaitingTermination.get() == 0 && completed) {
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

            // assert: system reached quiescent state - no reentry into drain() is expected
            return;
         }
      }
   }
}
