/*
 * StreamUtils.java
 * Copyright 2021 Rob Spoor
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

package com.github.robtimus.stream;

import static com.github.robtimus.stream.AdditionalCollectors.partitioning;
import static com.github.robtimus.stream.AdditionalCollectors.sequentialOnly;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * A utility class for streams.
 *
 * @author Rob Spoor
 */
public final class StreamUtils {

    private StreamUtils() {
    }

    /**
     * Reduces elements of a stream in partitions of a specific size, then runs an action for each reduction result.
     * This method can be described as "for every <em>count</em> elements of <em>stream</em>, collect using <em>collector</em>, then run
     * <em>action</em> on the collector's result".
     * <p>
     * Note: this method does not support concurrent streams.
     *
     * @param <T> The type of stream elements.
     * @param <R> The type of result of reducing each partition.
     * @param count The number of elements in each partition.
     * @param stream The {@link Stream} to reduce elements for.
     * @param collector The {@link Collector} to use for reducing partitions.
     * @param action The action to run for each reduction result.
     */
    public static <T, R> void forEvery(int count, Stream<T> stream, Collector<? super T, ?, R> collector, Consumer<? super R> action) {
        if (count < 1) {
            throw new IllegalArgumentException(count + " < 1"); //$NON-NLS-1$
        }
        Objects.requireNonNull(stream);
        Objects.requireNonNull(collector);
        Objects.requireNonNull(action);

        // Use a Collector, even though we're not actually collecting anything
        // The intermediate object (a) is ignored, and can therefore be null
        Collector<R, ?, ?> partitioner = sequentialOnly(() -> null, (a, t) -> action.accept(t));
        Object result = stream.collect(partitioning(count, collector, partitioner));
        assert result == null;
    }
}
