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

import java.util.concurrent.atomic.AtomicBoolean;

import io.helidon.common.mapper.Mapper;

/**
 * Processor of {@link Publisher} to {@link Single} that only processes the first
 * item and maps it to a different type.
 *
 * @param <T> subscribed type
 * @param <U> published type
 */
class SingleMappingProcessor<T, U> extends MapProcessor<T, U> implements Single<U> {
    private final AtomicBoolean requested = new AtomicBoolean(false);

    SingleMappingProcessor(Mapper<T, U> mapper) {
        super(mapper);
    }

    @Override
    public void request(long n) {
        // just making sure this Processor requests one and only one
        if (requested.get() || requested.getAndSet(true)) {
            return;
        }

        super.request(1);
    }

    protected void submit(T item) {
        super.submit(item);
        complete();
    }
}
