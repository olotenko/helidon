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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Flatten the elements emitted by publishers produced by the mapper function to this stream.
 *
 * @param <T> input item type
 * @param <X> output item type
 */
public class MultiFlatMapProcessor<T, X> extends BaseProcessor<T, X> implements Multi<X> {
    private static final int INNER_COMPLETE = 1;
    private static final int OUTER_COMPLETE = 2;
    private static final int NOT_STARTED = 4;
    private static final int ALL_COMPLETE = INNER_COMPLETE | OUTER_COMPLETE;

    private Function<T, Flow.Publisher<X>> mapper;
    private final AtomicInteger active = new AtomicInteger(NOT_STARTED | INNER_COMPLETE);
    private final Inner inner = new Inner();

    private volatile Backpressure backp = new Backpressure(new Flow.Subscription() {
            public void request(long n) {
                if (n <= 0) {
                    getSubscription().request(n); // let the Subscription deal with bad requests
                    return;
                }

                int a;
                do {
                    a = active.get();
                    if ((a & NOT_STARTED) == 0) {
                        return;
                    }
                } while(!active.compareAndSet(a, a - NOT_STARTED));

                getSubscription().request(1);
            }

            public void cancel() {}
        });

    /**
     * Create new {@link MultiFlatMapProcessor}.
     */
    protected MultiFlatMapProcessor() {
    }

    /**
     * Create new {@link MultiFlatMapProcessor} with item to {@link java.lang.Iterable} mapper.
     *
     * @param mapper to provide iterable for every item from upstream
     */
    protected MultiFlatMapProcessor(Function<T, Flow.Publisher<X>> mapper) {
        Objects.requireNonNull(mapper);
        this.mapper = mapper;
    }

    /**
     * Create new {@link MultiFlatMapProcessor} with item to {@link java.lang.Iterable} mapper.
     *
     * @param mapper to provide iterable for every item from upstream
     * @param <T>    input item type
     * @param <R>    output item type
     * @return {@link MultiFlatMapProcessor}
     */
    public static <T, R> MultiFlatMapProcessor<T, R> fromIterableMapper(Function<T, Iterable<R>> mapper) {
        return new MultiFlatMapProcessor<>(o -> Multi.from(mapper.apply(o)));
    }

    /**
     * Create new {@link MultiFlatMapProcessor} with item to {@link java.util.concurrent.Flow.Publisher} mapper.
     *
     * @param mapper to provide iterable for every item from upstream
     * @param <T>    input item type
     * @param <U>    output item type
     * @return {@link MultiFlatMapProcessor}
     */
    public static <T, U> MultiFlatMapProcessor<T, U> fromPublisherMapper(Function<T, Flow.Publisher<U>> mapper) {
        return new MultiFlatMapProcessor<>(mapper);
    }


    /**
     * Set mapper used for publisher creation.
     *
     * @param mapper function used for publisher creation
     * @return {@link MultiFlatMapProcessor}
     */
    protected MultiFlatMapProcessor<T, X> mapper(Function<T, Flow.Publisher<X>> mapper) {
        Objects.requireNonNull(mapper);
        this.mapper = mapper;
        return this;
    }

    protected void submit(T item) {
        active.set(0); // clear INNER_COMPLETE, if set; no other flags are set
        mapper.apply(item).subscribe(inner);
    }

    protected void complete(Throwable th) {
        setError(th);
        super.cancel();

        int a;
        do {
           a = active.get();
        } while(!active.compareAndSet(a, a | OUTER_COMPLETE));

        // the one who sets the second bit in ALL_COMPLETE must call super.complete
        // bit masking, because maybe also NOT_STARTED
        if ((a & ALL_COMPLETE) == INNER_COMPLETE) {
            super.complete(th);
            return;
        }
    }

    public void request(long n) {
        while(!backp.maybeRequest(n));
    }

    public void cancel() {
        backp.cancel();
        super.cancel();
    }

    protected static class Backpressure {
        protected final AtomicLong requested = new AtomicLong(0);
        protected final Flow.Subscription sub;

        public Backpressure(Flow.Subscription sub) {
            this.sub = sub;
        }

        public boolean maybeRequest(long n) {
            if (n <= 0) {
                sub.request(n); // let the Subscription deal with bad requests
                return true;
            }

            long r;
            do {
                r = requested.get();
                if (r < 0) {
                    return false;
                }
            } while(!requested.compareAndSet(r, Long.MAX_VALUE - r > n ? r + n: Long.MAX_VALUE));
            sub.request(n);
            return true;
        }

        public void deliver() {
            long r;
            do {
                r = requested.get();
            } while(r != Long.MAX_VALUE && !requested.compareAndSet(r, r-1));
        }

        public long terminate() {
            return requested.getAndSet(-1);
        }

        public void cancel() {
            sub.cancel();
        }
    }

    protected class Inner implements Flow.Subscriber<X> {
        public void onSubscribe(Flow.Subscription sub) {
            Backpressure old = backp;
            backp = new Backpressure(sub);
            long unused = old.terminate();

            if (unused == 0) {
                return;
            }

            request(unused);
        }

        public void onNext(X item) {
            backp.deliver();
            MultiFlatMapProcessor.this.subscriber.onNext(item);
        }

        public void onError(Throwable th) {
            // the one who sets the second bit in ALL_COMPLETE must call super.complete
            // set always succeeds to do this; should not wait for upstream to complete
            // i.e. it may be that active is either 0 or OUTER_COMPLETE;
            // MultiFlatMapProcessor.complete cannot enter super.complete, because it
            // cannot see INNER_COMPLETE set; and when it does, it will see both bits
            // are set.
            active.set(ALL_COMPLETE);
            MultiFlatMapProcessor.super.complete(th);
        }

        public void onComplete() {
            int a;
            do {
               a = active.get();
            } while(!active.compareAndSet(a, a | INNER_COMPLETE));

            // the one who sets the second bit in ALL_COMPLETE must call super.complete
            if (a == OUTER_COMPLETE) {
                MultiFlatMapProcessor.super.complete(getError());
                return;
            }

            MultiFlatMapProcessor.this.getSubscription().request(1);
        }
    }
}
