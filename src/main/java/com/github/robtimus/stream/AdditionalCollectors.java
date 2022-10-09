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
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;
import java.util.stream.Collectors;

/**
 * Additional {@link Collector} implementations.
 *
 * @author Rob Spoor
 */
public final class AdditionalCollectors {

    private static final BinaryOperator<Object> THROWING_COMBINER = (t1, t2) -> {
        throw new IllegalStateException(Messages.AdditionalCollectors.parallelStreamsNotSupported());
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
        return findSingle(() -> new IllegalStateException(Messages.AdditionalCollectors.multipleElements()));
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
            private T result = null;

            private void accumulate(T element) {
                Objects.requireNonNull(element);
                if (result != null) {
                    throw exceptionSupplier.get();
                }
                result = element;
            }

            private SingleValueCollector combine(SingleValueCollector other) {
                if (result != null && other.result != null) {
                    throw exceptionSupplier.get();
                }
                return result != null ? this : other;
            }

            private Optional<T> finish() {
                return Optional.ofNullable(result);
            }
        }

        return Collector.of(SingleValueCollector::new, SingleValueCollector::accumulate, SingleValueCollector::combine, SingleValueCollector::finish);
    }

    /**
     * Returns a {@link Collector} that finds a unique element of a stream, or {@link Optional#empty()} if the stream is empty.
     * Uniqueness is determined using {@link Object#equals(Object)}.
     * <p>
     * If the stream contains more than one different element, the returned {@link Collector} will throw an {@link IllegalStateException}.
     * If the stream contains {@code null} values, the returned {@link Collector} will throw a {@link NullPointerException}.
     *
     * @param <T> The type of input element.
     * @return A {@link Collector} that results the unique element of a stream
     */
    public static <T> Collector<T, ?, Optional<T>> findUnique() {
        return findUnique(() -> new IllegalStateException(Messages.AdditionalCollectors.multipleElements()));
    }

    /**
     * Returns a {@link Collector} that finds the unique element of a stream, or {@link Optional#empty()} if the stream is empty.
     * Uniqueness is determined using {@link Object#equals(Object)}.
     * <p>
     * If the stream contains more than one different element, the returned {@link Collector} will throw an exception provided by the given
     * {@link Supplier}. If the stream contains {@code null} values, the returned {@link Collector} will throw a {@link NullPointerException}.
     *
     * @param <T> The type of input element.
     * @param exceptionSupplier A {@link Supplier} for the exception to throw if more than one element is encountered.
     * @return A {@link Collector} that results the unique element of a stream
     */
    public static <T> Collector<T, ?, Optional<T>> findUnique(Supplier<? extends RuntimeException> exceptionSupplier) {
        Objects.requireNonNull(exceptionSupplier);

        final class SingleValueCollector {
            private T result = null;

            private void accumulate(T element) {
                Objects.requireNonNull(element);
                if (result == null) {
                    result = element;
                } else if (!result.equals(element)) {
                    throw exceptionSupplier.get();
                }
            }

            private SingleValueCollector combine(SingleValueCollector other) {
                if (result != null && other.result != null && !result.equals(other.result)) {
                    throw exceptionSupplier.get();
                }
                return result != null ? this : other;
            }

            private Optional<T> finish() {
                return Optional.ofNullable(result);
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
            throw new NullPointerException(Messages.AdditionalCollectors.toMap.nullValue(element));
        }
        addToMap(map, key, value);
    }

    private static <K, V> void addToMap(Map<K, V> map, K key, V value) {
        V existing = map.putIfAbsent(key, value);
        if (existing != null) {
            throw new IllegalStateException(Messages.AdditionalCollectors.toMap.duplicateKey(key));
        }
    }

    private static <K, V, M extends Map<K, V>> M combineMaps(M map1, M map2) {
        for (Map.Entry<K, V> entry : map2.entrySet()) {
            addToMap(map1, entry.getKey(), entry.getValue());
        }
        return map1;
    }

    /**
     * Returns a {@link Collector} that accumulates {@link CompletionStage} instances into a new {@link CompletableFuture}.
     * If the {@link CompletionStage} results need to be mapped or filtered before collecting, use {@link FutureValue} instead.
     *
     * @param <T> The result type of the {@link CompletionStage} instances.
     * @param <A> The intermediate accumulation type of the {@link Collector}.
     * @param <R> The result type of the collected {@link CompletableFuture}.
     * @param collector The collector for the {@link CompletionStage} results.
     * @return A {@link Collector} that collects {@link CompletionStage} instances.
     * @throws NullPointerException If the given {@link Collector} is {@code null}.
     * @since 1.1
     */
    public static <T, A, R> Collector<CompletionStage<T>, ?, CompletableFuture<R>> completionStages(Collector<T, A, R> collector) {
        Objects.requireNonNull(collector);

        final class CompletableFutureCollector {
            private CompletableFuture<A> result = CompletableFuture.completedFuture(collector.supplier().get());

            private void accumulate(CompletionStage<T> completionStage) {
                result = result.thenCombine(completionStage, (a, t) -> {
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
     * Returns a {@link Collector} that accumulates {@link CompletableFuture} instances into a new {@link CompletableFuture}.
     * If the {@link CompletableFuture} results need to be mapped or filtered before collecting, use {@link FutureValue} instead.
     *
     * @param <T> The result type of the {@link CompletableFuture} instances.
     * @param <A> The intermediate accumulation type of the {@link Collector}.
     * @param <R> The result type of the collected {@link CompletableFuture}.
     * @param collector The collector for the {@link CompletableFuture} results.
     * @return A {@link Collector} that collects {@link CompletableFuture} instances.
     * @throws NullPointerException If the given {@link Collector} is {@code null}.
     */
    @SuppressWarnings("unchecked")
    public static <T, A, R> Collector<CompletableFuture<T>, ?, CompletableFuture<R>> completableFutures(Collector<T, A, R> collector) {
        // THe collector returned by completionStages accepts CompletionStage<T>, so it also accepts CompletableFuture<T>
        return (Collector<CompletableFuture<T>, ?, CompletableFuture<R>>) (Collector<?, ?, ?>) completionStages(collector);
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
