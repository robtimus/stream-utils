/*
 * AdditionalCollectors.java
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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Additional {@link Collector} implementations.
 *
 * @author Rob Spoor
 */
public final class AdditionalCollectors {

    private static final BinaryOperator<Object> THROWING_COMBINER = (t1, t2) -> {
        throw new IllegalStateException(Messages.AdditionalCollectors.parallelStreamsNotSupported.get());
    };

    private AdditionalCollectors() {
    }

    /**
     * Returns a new {@link Collector} that only supports sequential streams.
     * If used in a parallel stream, the {@link Collector#combiner() combine operation} will throw an exception.
     *
     * @param <T> The type of input elements for the new {@link Collector}.
     * @param <R> The final result type of the new {@link Collector}.
     * @param supplier The supplier function for the new {@link Collector}.
     * @param accumulator The accumulator function for the new {@link Collector}.
     * @param characteristics The {@link Collector} characteristics for the new {@link Collector}.
     * @return The new {@link Collector}.
     * @throws NullPointerException If any argument is {@code null}.
     * @see Collector#of(Supplier, BiConsumer, BinaryOperator, Characteristics...)
     */
    public static <T, R> Collector<T, R, R> sequentialOnly(Supplier<R> supplier,
                                                           BiConsumer<R, T> accumulator,
                                                           Characteristics... characteristics) {

        @SuppressWarnings("unchecked")
        BinaryOperator<R> combiner = (BinaryOperator<R>) THROWING_COMBINER;
        return Collector.of(supplier, accumulator, combiner, characteristics);
    }

    /**
     * Returns a new {@link Collector} that only supports sequential streams.
     * If used in a parallel stream, the {@link Collector#combiner() combine operation} will throw an exception.
     *
     * @param <T> The type of input elements for the new {@link Collector}.
     * @param <A> The intermediate accumulation type of the new {@link Collector}.
     * @param <R> The final result type of the new {@link Collector}.
     * @param supplier The supplier function for the new {@link Collector}.
     * @param accumulator The accumulator function for the new {@link Collector}.
     * @param finisher The finisher function for the new {@link Collector}
     * @param characteristics The {@link Collector} characteristics for the new {@link Collector}.
     * @return The new {@link Collector}.
     * @throws NullPointerException If any argument is {@code null}.
     * @see Collector#of(Supplier, BiConsumer, BinaryOperator, Function, Characteristics...)
     */
    public static <T, A, R> Collector<T, A, R> sequentialOnly(Supplier<A> supplier,
                                                              BiConsumer<A, T> accumulator,
                                                              Function<A, R> finisher,
                                                              Characteristics... characteristics) {

        @SuppressWarnings("unchecked")
        BinaryOperator<A> combiner = (BinaryOperator<A>) THROWING_COMBINER;
        return Collector.of(supplier, accumulator, combiner, finisher, characteristics);
    }

    /**
     * Returns a {@link Collector} that finds the single element of a stream, or {@link Optional#empty()} if the stream is empty.
     * <p>
     * If the stream contains more than one element, the returned {@link Collector} will throw an {@link IllegalStateException}.
     * If the stream contains {@code null} values, the returned {@link Collector} will throw a {@link NullPointerException}.
     *
     * @param <T> The type of input element.
     * @return A {@link Collector} that results the single element of a stream
     */
    public static <T> Collector<T, ?, Optional<T>> findSingle() {
        return findSingle(() -> new IllegalStateException(Messages.AdditionalCollectors.multipleElements.get()));
    }

    /**
     * Returns a {@link Collector} that finds the single element of a stream, or {@link Optional#empty()} if the stream is empty.
     * <p>
     * If the stream contains more than one element, the returned {@link Collector} will throw an exception provided by the given {@link Supplier}.
     * If the stream contains {@code null} values, the returned {@link Collector} will throw a {@link NullPointerException}.
     *
     * @param <T> The type of input element.
     * @param exceptionSupplier A {@link Supplier} for the exception to throw if more than one element is encountered.
     * @return A {@link Collector} that results the single element of a stream
     */
    public static <T> Collector<T, ?, Optional<T>> findSingle(Supplier<? extends RuntimeException> exceptionSupplier) {
        Objects.requireNonNull(exceptionSupplier);

        final class SingleValueCollector {
            private Optional<T> result = Optional.empty();

            private void accumulate(T element) {
                result.ifPresent(t -> {
                    throw exceptionSupplier.get();
                });
                result = Optional.of(element);
            }

            private SingleValueCollector combine(SingleValueCollector other) {
                if (result.isPresent() && other.result.isPresent()) {
                    throw exceptionSupplier.get();
                }
                return result.isPresent() ? this : other;
            }

            private Optional<T> finish() {
                return result;
            }
        }

        return Collector.of(SingleValueCollector::new, SingleValueCollector::accumulate, SingleValueCollector::combine, SingleValueCollector::finish);
    }

    /**
     * Returns a {@link Collector} that accumulates elements into a {@link Map}.
     * This method is like {@link Collectors#toMap(Function, Function)} but with a custom map factory.
     * It's a convenient alternative to using {@link Collectors#toMap(Function, Function, BinaryOperator, Supplier)} without having to write the
     * merge function. Like {@link Collectors#toMap(Function, Function)}, the value mapping function cannot return {@code null} values.
     *
     * @param <T> The type of input element.
     * @param <K> The output type of the key mapping function.
     * @param <V> The output type of the value mapping function.
     * @param <M> The type of resulting {@link Map}.
     * @param keyMapper A mapping function to produce map keys.
     * @param valueMapper A mapping function to produce map values.
     * @param mapFactory A supplier providing a new empty {@link Map} into which the results will be inserted.
     * @return A {@link Collector} that accumulates elements into a {@link Map}.
     */
    public static <T, K, V, M extends Map<K, V>> Collector<T, ?, M> toMapWithSupplier(Function<? super T, ? extends K> keyMapper,
                                                                                      Function<? super T, ? extends V> valueMapper,
                                                                                      Supplier<M> mapFactory) {

        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(valueMapper);
        Objects.requireNonNull(mapFactory);

        return Collector.of(
                mapFactory,
                (m, e) -> addToMap(m, e, keyMapper, valueMapper),
                AdditionalCollectors::combineMaps
        );
    }

    private static <T, K, V> void addToMap(Map<K, V> map, T element,
                                           Function<? super T, ? extends K> keyMapper,
                                           Function<? super T, ? extends V> valueMapper) {

        K key = keyMapper.apply(element);
        V value = valueMapper.apply(element);
        if (value == null) {
            throw new NullPointerException(Messages.AdditionalCollectors.toMap.nullValue.get(element));
        }
        addToMap(map, key, value);
    }

    private static <K, V> void addToMap(Map<K, V> map, K key, V value) {
        V existing = map.putIfAbsent(key, value);
        if (existing != null) {
            throw new IllegalStateException(Messages.AdditionalCollectors.toMap.duplicateKey.get(key));
        }
    }

    private static <K, V, M extends Map<K, V>> M combineMaps(M map1, M map2) {
        for (Map.Entry<K, V> entry : map2.entrySet()) {
            addToMap(map1, entry.getKey(), entry.getValue());
        }
        return map1;
    }

    /**
     * Returns a {@link Collector} that filters elements before passing them to a downstream {@link Collector}.
     * <p>
     * In most cases, {@link Stream#filter(Predicate)} should be preferred over this method. However, there are some cases where the elements are not
     * available until they are collected. One such example is {@link #completableFutures(Collector)}. The {@link Stream#filter(Predicate)} method can
     * only be applied to the {@link CompletableFuture} instances, not their results. To apply filtering to the results, wrap the collector using this
     * method. For example, if {@code stream} is an existing {@code Stream<CompletableFuture<T>}:
     * <pre><code>
     * CompletableFuture&lt;List&lt;T&gt;&gt; result = stream.collect(completableFutures(filtering(Collectors.toList(), Objects::nonNull)));
     * </code></pre>
     * Here, {@code null} results are filtered out, whereas {@link Stream#filter(Predicate)} would only allow filtering out {@code null}
     * {@link CompletableFuture}s.
     *
     * @param <T> The type of input elements for the {@link Collector}.
     * @param <A> The intermediate accumulation type of the {@link Collector}.
     * @param <R> The final result type of the {@link Collector}.
     * @param downstream The downstream {@link Collector}.
     * @param filter The filter to apply to elements before passing them to the given downstream {@link Collector}.
     * @return A {@link Collector} that filters elements before passing them to the given downstream {@link Collector}.
     * @throws NullPointerException If the given downstream {@link Collector} or filter is {@code null}.
     */
    public static <T, A, R> Collector<T, A, R> filtering(Collector<T, A, R> downstream, Predicate<? super T> filter) {
        Objects.requireNonNull(downstream);
        Objects.requireNonNull(filter);

        Supplier<A> supplier = downstream.supplier();
        BiConsumer<A, T> accumulator = downstream.accumulator();
        BinaryOperator<A> combiner = downstream.combiner();
        Function<A, R> finisher = downstream.finisher();

        BiConsumer<A, T> filteringAccumulator = (a, t) -> {
            if (filter.test(t)) {
                accumulator.accept(a, t);
            }
        };

        return Collector.of(supplier, filteringAccumulator, combiner, finisher);
    }

    /**
     * Returns a {@link Collector} that accumulates {@link CompletableFuture} instances into a new {@link CompletableFuture}.
     *
     * @param <T> The result type of the {@link CompletableFuture} instances.
     * @param <A> The accumulator used by the collector for the {@link CompletableFuture} results.
     * @param <R> The result type of the collected {@link CompletableFuture}.
     * @param collector The collector for the {@link CompletableFuture} results.
     * @return A {@link Collector} that collects {@link CompletableFuture} instances.
     * @throws NullPointerException If the given {@link Collector} is {@code null}.
     */
    public static <T, A, R> Collector<CompletableFuture<T>, ?, CompletableFuture<R>> completableFutures(Collector<T, A, R> collector) {
        Objects.requireNonNull(collector);

        final class CompletableFutureCollector {
            private CompletableFuture<A> result = CompletableFuture.completedFuture(collector.supplier().get());

            private void accumulate(CompletableFuture<T> future) {
                result = result.thenCombine(future, (a, t) -> {
                    collector.accumulator().accept(a, t);
                    return a;
                });
            }

            private CompletableFutureCollector combine(CompletableFutureCollector other) {
                result = result.thenCombine(other.result, collector.combiner()::apply);
                return this;
            }

            private CompletableFuture<R> finish() {
                return result.thenApply(collector.finisher());
            }
        }

        return Collector.of(
                CompletableFutureCollector::new,
                CompletableFutureCollector::accumulate,
                CompletableFutureCollector::combine,
                CompletableFutureCollector::finish
        );
    }

    /**
     * Returns a {@link Collector} that partitions the input elements into portions of a maximum size. Each partition is fed into a downstream
     * {@link Collector}, and the results of this downstream {@link Collector} are fed into a partitioning {@link Collector}.
     * <p>
     * Note: the returned {@link Collector} does not support concurrent streams.
     *
     * @param <T> The type of input elements for the {@link Collector}.
     * @param <A1> The intermediate accumulation type of the downstream {@link Collector}.
     * @param <R1> The result type of the downstream {@link Collector}.
     * @param <A2> The intermediate accumulation type of the partitioning {@link Collector}.
     * @param <R2> The result type of the partitioning {@link Collector}.
     * @param partitionSize The maximum size of each partition.
     * @param downstream The downstream {@link Collector} to use.
     *                       It should not contain any state of its own other than the supplier, accumulator, combiner, finisher and characteristics.
     *                       Its supplier may be called several times, and each call should yield an "empty" object, and not contain any state from
     *                       any previous time it is called.
     * @param partitioner The partitioning {@link Collector} to use.
     * @return A {@link Collector} that partitions the input elements as described.
     * @throws IllegalArgumentException If the partition size is not at least 1.
     * @throws NullPointerException If either {@link Collector} is {@code null}.
     */
    public static <T, A1, R1, A2, R2> Collector<T, ?, R2> partitioning(int partitionSize, Collector<? super T, A1, R1> downstream,
            Collector<? super R1, A2, R2> partitioner) {

        if (partitionSize < 1) {
            throw new IllegalArgumentException(partitionSize + " < 1"); //$NON-NLS-1$
        }
        Objects.requireNonNull(downstream);
        Objects.requireNonNull(partitioner);

        final class Partitioner {
            private int currentPartitionSize = 0;

            private A1 downstreamIntermediate;

            private A2 partitionerIntermediate = partitioner.supplier().get();

            private void accumulate(T element) {
                if (downstreamIntermediate == null) {
                    downstreamIntermediate = downstream.supplier().get();
                }
                downstream.accumulator().accept(downstreamIntermediate, element);

                currentPartitionSize++;
                if (currentPartitionSize == partitionSize) {
                    finishPartition();
                }
            }

            private R2 finish() {
                if (currentPartitionSize > 0) {
                    finishPartition();
                }
                return partitioner.finisher().apply(partitionerIntermediate);
            }

            private void finishPartition() {
                R1 downstreamResult = downstream.finisher().apply(downstreamIntermediate);
                downstreamIntermediate = null;

                partitioner.accumulator().accept(partitionerIntermediate, downstreamResult);

                currentPartitionSize = 0;
            }
        }

        return sequentialOnly(Partitioner::new, Partitioner::accumulate, Partitioner::finish);
    }
}
