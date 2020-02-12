/*
 * Copyright (c) 2019, 2020 Oracle and/or its affiliates. All rights reserved.
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
 */
package io.helidon.common.reactive;

import java.util.Objects;

/**
 * Processor of {@link Multi} to {@link Single} that collects items from the {@link Multi} and publishes a single collector object
 * as a {@link Single}.
 *
 * @param <T> subscribed type (collected)
 * @param <U> published type (collector)
 */
final class MultiCollectingProcessor<T, U> extends BaseProcessor<T, U> implements Single<U> {

    private final Collector<T, U> collector;

    MultiCollectingProcessor(Collector<T, U> collector) {
        this.collector = Objects.requireNonNull(collector, "collector is null!");
    }

    @Override
    protected void submit(T item) {
        try {
            collector.collect(item);
        } catch (Throwable t) {
            // guarantee that the collector will receive all items produced by upstream,
            // even if it keeps failing
            super.setError(t);
        }
    }

    @Override
    protected void complete(Throwable ex) {
        if (ex != null) {
            super.complete(ex);
            return;
        }

        U value;
        try {
            // guarantee collector will always get value() called, if upstream did not fail
            // and subscription was not cancelled
            value = collector.value();

            if (getError() != null) {
                super.complete(getError());
                return;
            }

            // require collector to return non-null value(), if there were no errors collecting
            if (value == null) {
                throw new NullPointerException("Collector returned a null container");
            }
        } catch (Throwable t) {
            super.complete(t);
            return;
        }
        subscriber.onNext(value);
        subscriber.onComplete();
    }

    @Override
    public void request(long n) {
        // downstream is not expected to request more than one
        super.request(Long.MAX_VALUE);
    }
}
