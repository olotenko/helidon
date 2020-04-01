/*
 * Copyright (c)  2020 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.helidon.common.reactive;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Maps the upstream values into {@link java.util.concurrent.Flow.Publisher}s,
 * subscribes to some of them and funnels their events into a single sequence.
 * @param <T> the upstream element type
 * @param <R> the element type of the resulting and inner publishers
 */
final class MultiFlatMapPublisher<T, R> implements Multi<R> {

    protected static final VarHandle vherrors;
    protected static final VarHandle vhtail;
    protected static final VarHandle vhnext;

    static {
        VarHandle vh = null;
        VarHandle vht = null;
        VarHandle vhn = null;
        try {
            vh = MethodHandles.lookup().findVarHandle(FlatMapSubscriber.class,
                                                   "errors",
                                                   Throwable.class);
            vht = MethodHandles.lookup().findVarHandle(InnerQueue.class,
                                                   "tail",
                                                   Node.class);
            vhn = MethodHandles.lookup().findVarHandle(Node.class,
                                                   "next",
                                                   Node.class);
        } catch(Exception e) {
        }
        vherrors = vh;
        vhtail = vht;
        vhnext = vhn;
    }

    private final Multi<T> source;

    private final Function<? super T, ? extends Flow.Publisher<? extends R>> mapper;

    private final long maxConcurrency;

    private final long prefetch;

    private final boolean delayErrors;

    MultiFlatMapPublisher(Multi<T> source,
                          Function<? super T, ? extends Flow.Publisher<? extends R>> mapper,
                          long maxConcurrency, long prefetch, boolean delayErrors) {
        this.source = source;
        this.mapper = mapper;
        this.maxConcurrency = maxConcurrency;
        this.prefetch = prefetch;
        this.delayErrors = delayErrors;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super R> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber is null");
        source.subscribe(new FlatMapSubscriber<>(subscriber, mapper, maxConcurrency,
                prefetch, delayErrors));
    }

    static final class FlatMapSubscriber<T, R> extends AtomicInteger
    implements Flow.Subscriber<T>, Flow.Subscription {

        private final Flow.Subscriber<? super R> downstream;

        private final Function<? super T, ? extends Flow.Publisher<? extends R>> mapper;

        private final long maxConcurrency;

        private final long prefetch;

        private final long limit;

        private final boolean delayErrors;

        private Flow.Subscription upstream;

        private volatile boolean cancelPending; // when should cancel any pending items and enter terminal state

        protected volatile Throwable errors;

        private boolean canceled; // when should enter terminal state, but eventually stop signalling downstream

        private final ConcurrentMap<InnerSubscriber, Object> subscribers;

        protected final InnerQueue<InnerSubscriber> readReady = new InnerQueue<>();

        private final AtomicLong requested;

        private final AtomicLong awaitingTermination;

        private long emitted;

        FlatMapSubscriber(Flow.Subscriber<? super R> downstream,
                          Function<? super T, ? extends Flow.Publisher<? extends R>> mapper,
                          long maxConcurrency,
                          long prefetch,
                          boolean delayErrors) {
            this.downstream = downstream;
            this.mapper = mapper;
            this.maxConcurrency = maxConcurrency;
            this.prefetch = prefetch;
            this.limit = prefetch - (prefetch >> 2);
            this.delayErrors = delayErrors;
            this.subscribers = new ConcurrentHashMap<>();
            this.requested = new AtomicLong();
            this.awaitingTermination = new AtomicLong(1);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            Objects.requireNonNull(subscription);
            // FIXME: this is moot, because not atomic
            if (upstream != null) {
                subscription.cancel();
                throw new IllegalStateException("Subscription already set");
            }
            upstream = subscription;
            downstream.onSubscribe(this);
            subscription.request(maxConcurrency);
        }

        @Override
        public void onNext(T item) {
            if (cancelPending) {
                return;
            }

            Flow.Publisher<? extends R> innerSource;

            try {
                innerSource = Objects.requireNonNull(mapper.apply(item),
                        "The mapper returned a null Publisher");
            } catch (Throwable ex) {
                setError(ex);
                maybeDrain();
                return;
            }

            awaitingTermination.getAndIncrement();
            innerSource.subscribe(new InnerSubscriber());
        }

        @Override
        public void onError(Throwable throwable) {
            setError(throwable);
            complete();
        }

        protected void complete() {
            awaitingTermination.getAndDecrement();
            maybeDrain();
        }

        @Override
        public void onComplete() {
            complete();
        }

        void setError(Throwable throwable) {
            if (delayErrors) {
                addError(throwable);
            } else {
                vherrors.compareAndSet(this, null, throwable);
                cancelPending = true;
            }
        }

        @Override
        public void request(long n) {
            if (n <= 0L) {
                setError(new IllegalArgumentException("Rule §3.9 violated: non-positive request amount is forbidden"));
            } else {
                SubscriptionHelper.addRequest(requested, n);
            }
            maybeDrain();
        }

        @Override
        public void cancel() {
            canceled = true;
            cancelPending = true;
            maybeDrain();
        }

        protected void cleanup() {
            upstream.cancel();
            for (InnerSubscriber inner : subscribers.keySet()) {
                inner.cancel();
            }
            // assert: subscribers concurrent modification will ensure new InnerSubscribers are as good
            //         as after s.cancel()
            subscribers.clear();

            // assert: readReady modification is single-threaded
            // assert: no downstream.onNext is reachable after this readReady.clear()
            //         - it is called only after cancelPending is observed, and other
            //         accesses either ensure they are single-threaded (own readReady),
            //         or do not add to readReady, or will ensure cleanup() is called again
            //         (will ensure drain() is called)
            readReady.clear();
        }

        void addError(Throwable throwable) {
            for (;;) {
                Throwable ex = errors;
                if (ex == null) {
                    if (vherrors.compareAndSet(this, null, throwable)) {
                        return;
                    }
                } else if (ex instanceof FlatMapAggregateException) {
                    ex.addSuppressed(throwable);
                    return;
                } else {
                    Throwable newEx = new FlatMapAggregateException();
                    newEx.addSuppressed(ex);
                    newEx.addSuppressed(throwable);
                    if (vherrors.compareAndSet(this, ex, newEx)) {
                        return;
                    }
                }
            }
        }

        protected void maybeDrain() {
            if (getAndIncrement() == 0) {
                drain();
            }
        }

        protected boolean tryLockDrain() {
            return get() == 0 && compareAndSet(0, 1);
        }

        protected void drain() {
            // assert: all the concurrent changes of any variable of any object accessible from here will
            //         result in a change of drain lock
            for(int contenders = 1; contenders != 0; contenders = addAndGet(-contenders)) {
                boolean terminate = cancelPending;
                while(!terminate && emitted < requested.get() && !readReady.empty()) {
                    R value = readReady.poll().poll();

                    if (value == null) {
                        upstream.request(1L);
                    } else {
                        downstream.onNext(value);
                        emitted++;
                    }

                    terminate = cancelPending;
                }

                if (terminate) {
                    cleanup();
                    if (canceled) {
                        continue;
                    }
                }

                if (terminate || awaitingTermination.get() == 0 && readReady.empty()) {
                    // assert: terminate == cancelPending is set in two cases:
                    //         - cancel() called - in this case this line is not reachable, because cancelled
                    //           is observed above
                    //         - eager error signalling has been requested - in this case
                    //           error is set, and downstream.onError is expected to be called;
                    //           after this will behave like cancel() called
                    // assert: if awaitingTermination == 0, no more items will be added to readReady; if
                    //         readReady becomes empty, should signal the terminal state
                    canceled = true;
                    cancelPending = true;

                    if (errors != null) {
                        downstream.onError(errors);
                    } else {
                        downstream.onComplete();
                    }
                }
            }
        }

        /**
         * Instances of this class will be subscribed to the mapped inner
         * Publishers and calls back to the enclosing parent class.
         * @param <R> the element type of the inner sequence
         */
        final class InnerSubscriber
                implements Flow.Subscriber<R> {
            private Flow.Subscription sub;

            private long produced;

            // assert: innerQ accesses to head and tail are singlethreaded;
            // assert: put modifies tail, and uses of put ensure the cleanup of the queue
            //         happens eventually, if cancellation has been requested concurrently
            // assert: changes to tail.next become visible to the thread accessing head -
            //         a happens-before edge is established by readReady.put -> readReady.empty
            //         or subscribers.put -> subscribers.keySet
            // assert: changes to tail become visible to the thread accessing tail - on* are serialized
            // assert: empty, clear and poll do not modify tail; any concurrent changes to tail
            //         will be observed: the method that will observe these changes is always invoked
            private final InnerQueue<R> innerQ = new InnerQueue<>();

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                Objects.requireNonNull(subscription, "subscription is null");
                boolean first = subscribers.putIfAbsent(this, this) == null;
                if (!first || cancelPending) {
                    subscription.cancel();
                    if (first) {
                        subscribers.remove(this);
                        return;
                    }
                    throw new IllegalStateException("Subscription already set!");
                }
                this.sub = subscription;
                // assert: the cancellation loop will observe this subscriber
                subscription.request(prefetch);
            }

            @Override
            public void onNext(R item) {
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
                if (locked && readReady.empty() && !cancelPending && requested.get() > emitted) {
                    produced(1L);
                    downstream.onNext(item);
                    emitted++;
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

            @Override
            public void onError(Throwable throwable) {
                setError(throwable);
                complete();
            }

            protected void complete() {
                subscribers.remove(this);
                awaitingTermination.getAndDecrement();

                boolean locked = !cancelPending && innerQ.empty() && tryLockDrain();
                if (locked) {
                    upstream.request(1L);
                    drain();
                    return;
                }

                readReady.put(this);
                maybeDrain();
            }

            @Override
            public void onComplete() {
                complete();
            }

            public void produced(long n) {
                long p = produced + n;
                if (p >= limit) {
                    produced = 0L;
                    sub.request(p);
                } else {
                    produced = p;
                }
            }

            public void cancel() {
                if (sub == null) {
                    return;
                }
                sub.cancel();
                innerQ.clear();
            }

            public R poll() {
                produced(1L);
                return innerQ.poll();
            }

            @Override
            public String toString() {
                return "InnerSubscriber{"
                        + "cancelPending=" + cancelPending
                        + ", innerQ.empty=" + (innerQ.empty())
                        + '}';
            }
        }
    }

    /**
     * Used for aggregating multiple exceptions via the {@link #addSuppressed(Throwable)}
     * method.
     */
    static final class FlatMapAggregateException extends RuntimeException {
        @Override
        public synchronized Throwable fillInStackTrace() {
            return this; // No stacktrace of its own as it aggregates other exceptions
        }
    }

    // there probably is already a j.u.Queue that fits the bill, but need a guarantee
    // that put and poll are accessed single-threadedly, but not necessarily from the
    // same thread - i.e. put never touches head, and poll never touches tail.
    protected static class InnerQueue<X> {
        protected Node<X> head;
        protected volatile Node<X> tail;

        public InnerQueue() {
            head = tail = new Node<>();
        }

        public void putNotSafe(X item) {
            // assert: no concurrent calls to put() or putNotSafe()
            Node<X> n = new Node<>();
            n.v = item;
            Node<X> t = tail;
            vhnext.set(t, n);
            vhtail.set(this, n);
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
            vhnext.set(head, null);
        }

        public boolean empty() {
            return head.next == null;
        }
    }

    public static class Node<X> {
        public X v;
        public volatile Node<X> next;
    }
}
