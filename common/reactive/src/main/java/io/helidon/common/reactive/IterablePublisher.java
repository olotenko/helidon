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

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publisher from iterable, implemented as trampoline stack-less recursion.
 *
 * @param <T> item type
 */
class IterablePublisher<T> implements Flow.Publisher<T>, Flow.Subscription {
    private final Iterator<T> iterator;
    private final AtomicBoolean trampolineLock = new AtomicBoolean(false);
    private final AtomicLong requestCounter = new AtomicLong(0);
    private Throwable error;
    private Flow.Subscriber<? super T> subscriber;

    private IterablePublisher(Iterator<T> it) {
        iterator = it;
    }

    /**
     * Create new {@link IterablePublisher}.
     *
     * @param iterable to create publisher from
     * @param <T>      Item type
     * @return new instance of {@link IterablePublisher}
     */
    static <T> IterablePublisher<T> create(Iterable<T> iterable) {
        IterablePublisher<T> instance = new IterablePublisher<>(iterable.iterator());
        return instance;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        boolean contended = trampolineLock.getAndSet(true);

        if (contended || this.subscriber != null) {
            if (!contended) {
                trampolineLock.set(false);
                trySubmit();
            }

            subscriber.onSubscribe(EmptySubscription.INSTANCE);
            subscriber.onError(new IllegalStateException("This Publisher supports only one Subscriber"));
            return;
        }

        subscriber.onSubscribe(this);
        this.subscriber = subscriber;
        trampolineLock.set(false);
        trySubmit();
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
           error = new IllegalArgumentException("Expecting positive request, got " + n);
           requestCounter.getAndSet(-1);
           trySubmit();
           return;
        }

        long r;
        do {
           r = requestCounter.get();
           if (r < 0) {
              return;
           }
        } while(!requestCounter.compareAndSet(r, Long.MAX_VALUE - r > n ? r + n: Long.MAX_VALUE));

        trySubmit();
    }

    private void trySubmit() {
        long r;
        while((r = requestCounter.get()) != 0) {
            if (trampolineLock.get() || trampolineLock.getAndSet(true)) {
                return;
            }

            try {
                boolean hasNext;
                while ((hasNext = iterator.hasNext()) && r > 0) {
                    T next = iterator.next();
                    Objects.requireNonNull(next);
                    subscriber.onNext(next);
                    r = requestCounter.decrementAndGet();
                }

                if (r < 0) {
                    // cancel or error
                    if (error != null) {
                        throw error;
                    }
                    return;
                }

                if (!hasNext) {
                   subscriber.onComplete();
                   return;
                }
            } catch (Throwable th) {
                subscriber.onError(th);
                return;
            }
            trampolineLock.set(false);
        }
    }

    @Override
    public void cancel() {
        requestCounter.getAndSet(-1);
    }
}
