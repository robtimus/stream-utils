/*
 * FutureValue.java
 * Copyright 2022 Rob Spoor
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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class that allows filtering and mapping on {@link CompletableFuture} instances inside streams.
 * <p>
 * In streams, applying operations such as {@link Stream#filter(Predicate)} immediately alter the state of the streams. When using streams of
 * {@link CompletableFuture}, the result is not available when the stream operation is applied. This class is meant to help with that issue as far as
 * possible. It does so by providing the most essential functionality using asynchronous mapping:
 * <ul>
 * <li>Filtering values. However, because the actual filtering is done asynchronously, instead of using {@link Stream#filter(Predicate)},
 *     {@link Stream#map(Function)} should be used in combination with {@link #filter(Predicate)}.</li>
 * <li>Transforming values, using a combination of {@link Stream#map(Function)} and {@link #map(Function)}.</li>
 * <li>Collecting values, using a combination of {@link Stream#collect(Collector)} and {@link #collect(Collector)}.</li>
 * <li>Running an action on values, using a combination of {@link Stream#forEach(Consumer)} and {@link #run(Consumer)}.</li>
 * </ul>
 * Most other operations rely on the internal state of the stream to change. These operations should not be used after the first mapping to
 * {@link #wrap(CompletableFuture)}. The only stream operations that can safely be used are {@link Stream#map(Function)} (both for mapping and
 * filtering), {@link Stream#forEach(Consumer)} and {@link Stream#collect(Collector)}. For other methods, if possible, use
 * {@link Stream#collect(Collector)} as replacement. For instance, the following can be used as replacement for {@link Stream#reduce(BinaryOperator)}
 * using {@link Collectors#reducing(BinaryOperator)}:
 * <pre><code>
 * CompletableFuture&lt;Optional&lt;Integer&gt;&gt; result = stream
 *         .map(FutureValue::wrap)
 *         .map(FutureValue.filter(i -&gt; (i &amp; 1) == 0))
 *         .collect(FutureValue.collect(reducing(Integer::sum));
 * </code></pre>
 *
 * @author Rob Spoor
 * @param <T> The type of {@link CompletableFuture} result.
 */
public final class FutureValue<T> {

    private final CompletableFuture<ValueHolder<T>> future;

    private FutureValue(CompletableFuture<ValueHolder<T>> future) {
        this.future = future;
    }

    /**
     * Wraps a {@link CompletableFuture} in a {@code FutureValue}. This is usually used in {@link Stream#map(Function)} to start using this class.
     *
     * @param <T> The type of {@link CompletableFuture} result.
     * @param future The future to wrap.
     * @return A {@code FutureValue} wrapping the given {@link CompletableFuture}.
     * @throws NullPointerException If the given {@link CompletableFuture} is {@code null}.
     */
    public static <T> FutureValue<T> wrap(CompletableFuture<T> future) {
        return new FutureValue<>(future.thenApply(ValueHolder::new));
    }

    /**
     * Returns a unary operator that applies filtering to a stream of {@code FutureValue}. However, because the actual filtering is done
     * asynchronously, this must be applied using {@link Stream#map(Function)} and not using {@link Stream#filter(Predicate)}.
     *
     * @param <T> The type of {@link CompletableFuture} result.
     * @param predicate The predicate to apply.
     * @return A unary operator that applies filtering to a stream of {@code FutureValue}.
     * @throws NullPointerException If the given predicate is {@code null}.
     */
    public static <T> UnaryOperator<FutureValue<T>> filter(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate);
        return value -> new FutureValue<>(value.future.thenApply(holder -> holder.filter(predicate)));
    }

    /**
     * Returns a function that transforms one {@code FutureValue} instance into another. This is usually used in a call to
     * {@link Stream#map(Function)} to transform {@link CompletableFuture} result.
     *
     * @param <T> The type of the input to the function.
     * @param <R> The type of the result of the function.
     * @param mapper The function to apply to each {@link CompletableFuture} result.
     * @return A function that transforms one {@code FutureValue} instance into another
     * @throws NullPointerException If the given function is {@code null}.
     */
    public static <T, R> Function<FutureValue<T>, FutureValue<R>> map(Function<? super T, ? extends R> mapper) {
        Objects.requireNonNull(mapper);
        return value -> new FutureValue<>(value.future.thenApply(holder -> holder.map(mapper)));
    }

    /**
     * Returns a {@link Collector} that accumulates {@code FutureValue} instances into a {@link CompletableFuture}.
     * This method is similar to {@link AdditionalCollectors#completableFutures(Collector)}. That method can be used if no filtering or mapping is
     * needed on the {@link CompletableFuture} results. If only one filter operation is needed, that method can also be used in combination with
     * {@link AdditionalCollectors#filtering(Collector, Predicate)}.
     *
     * @param <T> The result type of the {@link CompletableFuture} instances.
     * @param <A> The intermediate accumulation type of the {@link Collector}.
     * @param <R> The result type of the collected {@link CompletableFuture}.
     * @param collector The collector for the {@link CompletableFuture} results.
     * @return A {@link Collector} that collects {@code FutureValue} instances.
     * @throws NullPointerException If the given {@link Collector} is {@code null}.
     */
    public static <T, A, R> Collector<FutureValue<T>, ?, CompletableFuture<R>> collect(Collector<T, A, R> collector) {
        Objects.requireNonNull(collector);

        class FutureValueCollector {
            private CompletableFuture<ValueHolder<A>> result = CompletableFuture.completedFuture(new ValueHolder<>(collector.supplier().get()));

            private void accumulate(FutureValue<T> value) {
                result = result.thenCombine(value.future, (a, t) -> {
                    if (!t.filtered) {
                        collector.accumulator().accept(a.value, t.value);
                    }
                    return a;
                });
            }

            private FutureValueCollector combine(FutureValueCollector other) {
                result = result.thenCombine(other.result, (v1, v2) -> new ValueHolder<>(collector.combiner().apply(v1.value, v2.value)));
                return this;
            }

            private CompletableFuture<R> finish() {
                return result.thenApply(holder -> collector.finisher().apply(holder.value));
            }
        }

        return Collector.of(
                FutureValueCollector::new,
                FutureValueCollector::accumulate,
                FutureValueCollector::combine,
                FutureValueCollector::finish
        );
    }

    /**
     * Returns a consumer that performs an action on {@code FutureValue} instances. This is usually used in a call to {@link Stream#forEach(Consumer)}
     * or {@link Stream#forEachOrdered(Consumer)}.
     * <p>
     * Although this method can be used in a call to {@link Stream#peek(Consumer)}, the result is unpredictable due to the asynchronous nature of
     * {@link CompletableFuture}s. The same goes for {@link Stream#forEachOrdered(Consumer)}.
     *
     * @param <T> The type of {@link CompletableFuture} result.
     * @param action The action to perform for each {@link CompletableFuture} result. Note that the action is called asynchronously.
     * @return A consumer that performs the given action on {@code FutureValue} instances asynchronously.
     * @throws NullPointerException If the given action is {@code null}.
     */
    public static <T> Consumer<FutureValue<T>> run(Consumer<? super T> action) {
        Objects.requireNonNull(action);
        return value -> value.future.thenAccept(holder -> holder.run(action));
    }

    private static final class ValueHolder<T> {

        private static final ValueHolder<Object> FILTERED_HOLDER = new ValueHolder<>();

        private final T value;
        private final boolean filtered;

        private ValueHolder(T value) {
            this.value = value;
            this.filtered = false;
        }

        private ValueHolder() {
            this.value = null;
            this.filtered = true;
        }

        private ValueHolder<T> filter(Predicate<? super T> predicate) {
            return filtered || !predicate.test(value) ? filtered() : this;
        }

        private <R> ValueHolder<R> map(Function<? super T, ? extends R> mapper) {
            return filtered ? filtered() : new ValueHolder<>(mapper.apply(value));
        }

        private void run(Consumer<? super T> action) {
            if (!filtered) {
                action.accept(value);
            }
        }

        @SuppressWarnings("unchecked")
        private static <T> ValueHolder<T> filtered() {
            return (ValueHolder<T>) FILTERED_HOLDER;
        }
    }
}
