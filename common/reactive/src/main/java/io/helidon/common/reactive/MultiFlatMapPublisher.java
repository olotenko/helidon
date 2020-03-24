package io.helidon.common.reactive;

public class MultiFlatMapPublisher<X> implements Flow.Publisher<X>, Flow.Subscription {
   protected ConcurrentLinkedQueue<InnerSubscriber> readReady = new ConcurrentLinkedQueue<>();
   protected Throwable error;
   protected volatile boolean cancelled;
   protected volatile boolean completed;

   protected static final Object EOF = new Object();

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

      drain();
   }

   public void cancel() {
      cancelled = true;
      sub.cancel();
      drain();
   }

   protected void cleanup() {
      for(InnerSubscriber s: concurrentSubs.keySet()) {
         s.cancel();
         concurrentSubs.remove(s);
      }

      // assert: InnerSubscribers will not add items that should have appeared after those
      //         currently accessible via readReady.
      readReady.clear(); // this does not eliminate the need for draining
   }

   protected void complete() {
      completed = true;
      drain();
   }

   protected class InnerSubscriber implements Flow.Subscriber<X> {
      protected Flow.Subscription sub;
      protected int consumed;
      protected ConcurrentLinkedQueue<X> innerQ = new ConcurrentLinkedQueue<>();

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
         drain();
      }

      public void onNext(X item) {
         if (cancelled) {
            return;
         }

         innerQ.put(item);
         readReady.put(this);
         drain();
      }

      public X poll() {
         consumed++;
         if (consumed >= limit) {
            int p = consumed;
            consumed = 0;
            sub.request(p); // assert: if cancelled, no-op
         }

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
         head = tail = new Node(null);
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
   }

   public static class Node<X> {
      public X v;
      public Node<X> next;
   }

   protected void drain() {
      if (drainLock.getAndIncrement() != 0) {
         return;
      }

      for(int contenders = 1; contenders != 0; contenders = drainLock.addAndGet(-contenders)) {
         long delivered;
         for(delivered = 0; !cancelled && delivered < requested.get() && !readReady.empty();) {
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
