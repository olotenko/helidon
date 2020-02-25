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
    private Iterator<T> iterator;
    private final AtomicBoolean subscriberLock = new AtomicBoolean(false);
    private final AtomicLong requestCounter = new AtomicLong(0);
    private volatile Throwable error;
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
    static <T> Flow.Publisher<T> create(Iterable<T> iterable) {
        Flow.Publisher<T> instance;
        try {
            Iterator<T> it = iterable.iterator();
            if (!it.hasNext()) {
                return MultiEmpty.instance();
            }

            instance = new IterablePublisher<>(it);
        } catch(Throwable th) {
            instance = MultiError.create(th);
        }
        return instance;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        boolean contended = subscriberLock.getAndSet(true);

        if (contended) {
            subscriber.onSubscribe(EmptySubscription.INSTANCE);
            subscriber.onError(new IllegalStateException("This Publisher supports only one Subscriber"));
            return;
        }

        this.subscriber = subscriber;
        subscriber.onSubscribe(this);
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
           Throwable tmp = new IllegalArgumentException("Expecting positive request, got " + n);
           error = tmp;
           trySubmit(requestCounter.getAndSet(-1));
           return;
        }

        long r;
        do {
           r = requestCounter.get();
           if (r < 0) {
              return;
           }
        } while(!requestCounter.compareAndSet(r, Long.MAX_VALUE - r > n ? r + n: Long.MAX_VALUE));

        trySubmit(r);
    }

    private void trySubmit(long lastSeen) {
        if (lastSeen != 0) {
            // assert: all updates to requestCounter outside trySubmit are followed by trySubmit, when on* signals are expected
            // assert: when lastSeen != 0, it means some trySubmit has not reached the line
            //         updating requestCounter - only that line can reduce it to 0
            //         all other changes either make it negative, or increment positive values.
            return;
        }


        try {
            long nexted;
            do {
               // assert: requestCounter.get() is never 0
               for (nexted = 0; nexted < requestCounter.get(); nexted++) {
                   // assert: iterator.hasNext() has been seen true
                   //         see Multi.from(...) - IterablePublisher is created only for iterators
                   //         that hasNext; this loop also checks hasNext() before allowing further onNext
                   T next = iterator.next();
                   Objects.requireNonNull(next);
                   subscriber.onNext(next);

                   if (!iterator.hasNext()) {
                      iterator = null;
                      subscriber.onComplete();
                      return;
                   }
               }

               // assert: no positive values of nexted can overflow subtraction back to a positive
               //         number, even if requestCounter == -1 - so concurrent updaters can't observe
               //         a positive number after a negative number has been set once
               lastSeen = requestCounter.addAndGet(-nexted);
            } while(lastSeen > 0);

            // cancel, error, or no more requests
            if (lastSeen < 0) {
                // assert: if lastSeen == 0, then non-null error indicates there is a future invocation of
                //         trySubmit that will throw; so need to clean up iterator reference and observe
                //         error only if there has been a request or cancel before requestCounter update by
                //         trySubmit
                iterator = null;
                if (error != null) {
                    throw error;
                }
            }
        } catch (Throwable th) {
            iterator = null;
            subscriber.onError(th);
            return;
        }
    }

    @Override
    public void cancel() {
        requestCounter.getAndSet(-1);
    }
}
