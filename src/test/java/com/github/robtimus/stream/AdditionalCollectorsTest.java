/*
 * AdditionalCollectorsTest.java
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

import static com.github.robtimus.stream.AdditionalCollectors.completableFutures;
import static com.github.robtimus.stream.AdditionalCollectors.filtering;
import static com.github.robtimus.stream.AdditionalCollectors.findSingle;
import static com.github.robtimus.stream.AdditionalCollectors.partitioning;
import static com.github.robtimus.stream.AdditionalCollectors.sequentialOnly;
import static com.github.robtimus.stream.AdditionalCollectors.toMapWithSupplier;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("nls")
class AdditionalCollectorsTest {

    @Nested
    @DisplayName("sequentialOnly")
    class SequentialOnly {

        @Nested
        @DisplayName("without finisher")
        class WithoutFinisher {

            @Test
            @DisplayName("sequential")
            void testSequential() {
                int count = 50;

                Collector<Integer, ?, List<Integer>> collector = sequentialOnly(ArrayList::new, (list, element) -> list.add(element));
                List<Integer> result = IntStream.range(0, count)
                        .map(i -> i * i)
                        .boxed()
                        .collect(collector);

                List<Integer> expected = IntStream.range(0, count)
                        .map(i -> i * i)
                        .boxed()
                        .collect(toList());

                assertEquals(expected, result);
            }

            @Test
            @DisplayName("parallel")
            void testParallel() {
                int count = 50;

                Collector<Integer, ?, List<Integer>> collector = sequentialOnly(ArrayList::new, (list, element) -> list.add(element));
                Stream<Integer> stream = IntStream.range(0, count)
                        .map(i -> i * i)
                        .boxed()
                        .parallel();

                assertParallelStreamNotSupported(stream, collector);
            }
        }

        @Nested
        @DisplayName("with finisher")
        class WithFinisher {

            @Test
            @DisplayName("sequential")
            void testSequential() {
                int count = 50;

                Collector<Integer, ?, Integer> collector = sequentialOnly(ArrayList::new, (list, element) -> list.add(element), this::sum);
                int result = IntStream.range(0, count)
                        .map(i -> i * i)
                        .boxed()
                        .collect(collector);

                int expected = IntStream.range(0, count)
                        .map(i -> i * i)
                        .sum();

                assertEquals(expected, result);
            }

            @Test
            @DisplayName("parallel")
            void testParallel() {
                int count = 50;

                Collector<Integer, ?, Integer> collector = sequentialOnly(ArrayList::new, (list, element) -> list.add(element), this::sum);
                Stream<Integer> stream = IntStream.range(0, count)
                        .map(i -> i * i)
                        .boxed()
                        .parallel();

                assertParallelStreamNotSupported(stream, collector);
            }

            private int sum(List<Integer> list) {
                return list.stream()
                        .mapToInt(i -> i)
                        .sum();
            }
        }
    }

    @Nested
    @DisplayName("findSingle")
    class FindSingle {

        @Test
        @DisplayName("empty stream")
        void testEmptyStream() {
            Stream<Integer> stream = Stream.empty();
            Optional<Integer> result = stream.collect(findSingle());
            assertEquals(Optional.empty(), result);
        }

        @Test
        @DisplayName("stream with single element")
        void testSingleElement() {
            Optional<Integer> result = IntStream.range(0, 10)
                    .boxed()
                    .filter(i -> i == 0)
                    .collect(findSingle());
            assertEquals(Optional.of(0), result);
        }

        @Test
        @DisplayName("stream with multiple elements")
        void testMultipleElements() {
            Stream<Integer> stream = IntStream.range(0, 10)
                    .boxed();
            Collector<Integer, ?, Optional<Integer>> collector = findSingle();

            IllegalStateException exception = assertThrows(IllegalStateException.class, () -> stream.collect(collector));
            assertEquals(Messages.AdditionalCollectors.multipleElements.get(), exception.getMessage());
        }

        @Nested
        @DisplayName("combine")
        class Combine {

            @Test
            @DisplayName("empty intermediates")
            void testCombineEmptyIntermediates() {
                testCombineEmptyIntermediates(findSingle());
            }

            private <A> void testCombineEmptyIntermediates(Collector<Integer, A, Optional<Integer>> collector) {
                A intermediate1 = collector.supplier().get();
                A intermediate2 = collector.supplier().get();

                A combined = collector.combiner().apply(intermediate1, intermediate2);

                Optional<Integer> result = collector.finisher().apply(combined);
                assertEquals(Optional.empty(), result);
            }

            @Test
            @DisplayName("non-empty intermediate with empty intermediate")
            void testCombineNonEmptyIntermediateWithEmptyIntermediate() {
                testCombineNonEmptyIntermediateWithEmptyIntermediate(findSingle());
            }

            private <A> void testCombineNonEmptyIntermediateWithEmptyIntermediate(Collector<Integer, A, Optional<Integer>> collector) {
                A intermediate1 = collector.supplier().get();
                A intermediate2 = collector.supplier().get();

                collector.accumulator().accept(intermediate1, 1);

                A combined = collector.combiner().apply(intermediate1, intermediate2);

                Optional<Integer> result = collector.finisher().apply(combined);
                assertEquals(Optional.of(1), result);
            }

            @Test
            @DisplayName("empty intermediate with non-empty intermediate")
            void testCombineEmptyIntermediateWithNonEmptyIntermediate() {
                testCombineEmptyIntermediateWithNonEmptyIntermediate(findSingle());
            }

            private <A> void testCombineEmptyIntermediateWithNonEmptyIntermediate(Collector<Integer, A, Optional<Integer>> collector) {
                A intermediate1 = collector.supplier().get();
                A intermediate2 = collector.supplier().get();

                collector.accumulator().accept(intermediate2, 2);

                A combined = collector.combiner().apply(intermediate1, intermediate2);

                Optional<Integer> result = collector.finisher().apply(combined);
                assertEquals(Optional.of(2), result);
            }

            @Test
            @DisplayName("non-empty intermediates")
            void testCombineNonEmptyIntermediates() {
                testCombineNonEmptyIntermediates(findSingle());
            }

            private <A> void testCombineNonEmptyIntermediates(Collector<Integer, A, Optional<Integer>> collector) {
                A intermediate1 = collector.supplier().get();
                A intermediate2 = collector.supplier().get();

                collector.accumulator().accept(intermediate1, 1);
                collector.accumulator().accept(intermediate2, 2);

                BinaryOperator<A> combiner = collector.combiner();

                IllegalStateException exception = assertThrows(IllegalStateException.class, () -> combiner.apply(intermediate1, intermediate2));
                assertEquals(Messages.AdditionalCollectors.multipleElements.get(), exception.getMessage());
            }
        }

        @Test
        @DisplayName("with null exception supplier")
        void testNullExceptionSupplier() {
            assertThrows(NullPointerException.class, () -> findSingle(null));
        }
    }

    @Nested
    @DisplayName("toMapWithSupplier")
    class ToMapWithSupplier {

        @Nested
        @DisplayName("no duplicates")
        class NoDuplicates {

            @Test
            @DisplayName("collect")
            void testCollect() {
                Map<Integer, String> result = IntStream.range(0, 20)
                        .mapToObj(i -> i)
                        .collect(toMapWithSupplier(Function.identity(), Object::toString, TreeMap::new));

                Map<Integer, String> expected = new HashMap<>();
                for (int i = 0; i < 20; i++) {
                    expected.put(i, Integer.toString(i));
                }

                assertEquals(expected, result);
                assertInstanceOf(TreeMap.class, result);
            }

            @Test
            @DisplayName("collect parallel")
            void testCollectParallel() {
                Map<Integer, String> result = IntStream.range(0, 20)
                        .parallel()
                        .mapToObj(i -> i)
                        .collect(toMapWithSupplier(Function.identity(), Object::toString, TreeMap::new));

                Map<Integer, String> expected = new HashMap<>();
                for (int i = 0; i < 20; i++) {
                    expected.put(i, Integer.toString(i));
                }

                assertEquals(expected, result);
                assertInstanceOf(TreeMap.class, result);
            }
        }

        @Nested
        @DisplayName("duplicates")
        class Duplicates {

            @Test
            @DisplayName("collect")
            void testCollect() {
                Stream<Integer> stream = IntStream.range(0, 20)
                        .mapToObj(i -> i);
                Collector<Integer, ?, ?> collector = toMapWithSupplier(i -> i % 10, Object::toString, TreeMap::new);
                IllegalStateException exception = assertThrows(IllegalStateException.class, () -> stream.collect(collector));

                String messagePattern = Messages.AdditionalCollectors.toMap.duplicateKey.get(0).replace("0", "\\d+");
                assertThat(exception.getMessage(), matchesPattern(messagePattern));
            }

            @Test
            @DisplayName("collect parallel")
            void testCollectParallel() {
                Stream<Integer> stream = IntStream.range(0, 20)
                        .parallel()
                        .mapToObj(i -> i);
                Collector<Integer, ?, ?> collector = toMapWithSupplier(i -> i % 10, Object::toString, TreeMap::new);
                IllegalStateException exception = assertThrows(IllegalStateException.class, () -> stream.collect(collector));

                String messagePattern = Messages.AdditionalCollectors.toMap.duplicateKey.get(0).replace("0", "\\d+");
                // ForkJoinTask may wrap the IllegalStateException in another IllegalStateException
                // Add an optional IllegalStateException before the message
                messagePattern = "(" + Pattern.quote(IllegalStateException.class.getName()) + ": )?" + messagePattern;
                assertThat(exception.getMessage(), matchesPattern(messagePattern));
            }
        }

        @Nested
        @DisplayName("null values")
        class NullValues {

            @Test
            @DisplayName("collect")
            void testCollect() {
                Stream<Integer> stream = IntStream.range(0, 20)
                        .mapToObj(i -> i);
                Collector<Integer, ?, ?> collector = toMapWithSupplier(i -> i % 10, i -> i == 10 ? null : i.toString(), TreeMap::new);
                NullPointerException exception = assertThrows(NullPointerException.class, () -> stream.collect(collector));

                String messagePattern = Messages.AdditionalCollectors.toMap.nullValue.get(0).replace("0", "\\d+");
                assertThat(exception.getMessage(), matchesPattern(messagePattern));
            }

            @Test
            @DisplayName("collect parallel")
            void testCollectParallel() {
                Stream<Integer> stream = IntStream.range(0, 20)
                        .parallel()
                        .mapToObj(i -> i);
                Collector<Integer, ?, ?> collector = toMapWithSupplier(i -> i % 10, i -> i == 10 ? null : i.toString(), TreeMap::new);
                NullPointerException exception = assertThrows(NullPointerException.class, () -> stream.collect(collector));

                // ForkJoinTask may wrap the NullPointerException in another NullPointerException
                // Add an optional NullPointerException before the message
                if (exception.getCause() instanceof NullPointerException) {
                    exception = (NullPointerException) exception.getCause();
                }

                String messagePattern = Messages.AdditionalCollectors.toMap.nullValue.get(0).replace("0", "\\d+");
                assertThat(exception.getMessage(), matchesPattern(messagePattern));
            }
        }

        @Test
        @DisplayName("with null key mapper")
        void testNullKeyMapper() {
            Function<Integer, Integer> valueMapper = Function.identity();
            Supplier<Map<Integer, Integer>> mapSupplier = HashMap::new;
            assertThrows(NullPointerException.class, () -> toMapWithSupplier(null, valueMapper, mapSupplier));
        }

        @Test
        @DisplayName("with null value mapper")
        void testNullValueMapper() {
            Function<Integer, Integer> keyMapper = Function.identity();
            Supplier<Map<Integer, Integer>> mapSupplier = HashMap::new;
            assertThrows(NullPointerException.class, () -> toMapWithSupplier(keyMapper, null, mapSupplier));
        }

        @Test
        @DisplayName("with null map supplier")
        void testNullMapSupplier() {
            Function<Integer, Integer> keyMapper = Function.identity();
            Function<Integer, Integer> valueMapper = Function.identity();
            assertThrows(NullPointerException.class, () -> toMapWithSupplier(keyMapper, valueMapper, null));
        }
    }

    @Nested
    @DisplayName("filtering")
    class Filtering {

        // filtering functionality is tested as part of CompletableFuture testing

        @Test
        @DisplayName("with null collector")
        void testNullCollector() {
            Predicate<Integer> filter = Objects::nonNull;
            assertThrows(NullPointerException.class, () -> filtering(null, filter));
        }

        @Test
        @DisplayName("with null filter")
        void testNullFilter() {
            Collector<Integer, ?, List<Integer>> collector = toList();
            assertThrows(NullPointerException.class, () -> filtering(collector, null));
        }
    }

    @Nested
    @DisplayName("completableFutures")
    class CompletableFutures {

        private int threadPoolSize = 5;

        private ExecutorService executor;

        @BeforeEach
        void setupExecutor() {
            executor = Executors.newFixedThreadPool(threadPoolSize);
        }

        @AfterEach
        void shutdownExecutor() {
            executor.shutdown();
        }

        @Test
        @DisplayName("collect")
        void testCollect() {
            int size = 2 * threadPoolSize;

            CompletableFuture<List<Integer>> result = IntStream.range(0, size)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                    .collect(completableFutures(toList()));

            List<Integer> expected = IntStream.range(0, size)
                    .map(i -> i * i)
                    .boxed()
                    .collect(toList());

            List<Integer> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

            assertEquals(expected, resultList);
        }

        @Test
        @DisplayName("collect filtering")
        void testCollectFiltering() {
            int size = 20;

            CompletableFuture<List<Integer>> result = IntStream.range(0, size)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                    .collect(completableFutures(filtering(toList(), i -> (i & 1) == 0)));

            List<Integer> expected = IntStream.range(0, size)
                    .map(i -> i * i)
                    .filter(i -> (i & 1) == 0)
                    .boxed()
                    .collect(toList());

            List<Integer> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

            assertEquals(expected, resultList);
        }

        @Test
        @DisplayName("collect parallel")
        void testCollectParallel() {
            int size = 2 * threadPoolSize;

            CompletableFuture<List<Integer>> result = IntStream.range(0, size)
                    .parallel()
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                    .collect(completableFutures(toList()));

            List<Integer> expected = IntStream.range(0, size)
                    .map(i -> i * i)
                    .boxed()
                    .collect(toList());

            List<Integer> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

            assertEquals(expected, resultList);
        }

        private int calculate(int i) {
            try {
                Thread.sleep(10 * i);
                return i * i;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        @Test
        @DisplayName("with null collector")
        void testNullCollector() {
            assertThrows(NullPointerException.class, () -> completableFutures(null));
        }
    }

    @Nested
    @DisplayName("partitioning")
    class Partitioning {

        @Nested
        @DisplayName("empty streams")
        class EmptyStreams {

            @Test
            @DisplayName("partition to list of lists")
            void testListOfLists() {
                Collector<Integer, ?, List<List<Integer>>> collector = partitioning(10, toList(), toList());
                List<List<Integer>> result = Stream.<Integer>empty().collect(collector);

                List<List<Integer>> expected = Collections.emptyList();

                assertEquals(expected, result);
            }

            @Test
            @DisplayName("partition to list of strings")
            void testListOfStrings() {
                Collector<String, ?, List<String>> collector = partitioning(10, joining(","), toList());
                List<String> result = Stream.<String>empty().collect(collector);

                List<String> expected = Collections.emptyList();

                assertEquals(expected, result);
            }

            @Test
            @DisplayName("partition to multi-line string")
            void testMultilineString() {
                Collector<String, ?, String> collector = partitioning(10, joining(","), joining("\n"));
                String result = Stream.<String>empty().collect(collector);

                String expected = "";

                assertEquals(expected, result);
            }
        }

        @Nested
        @DisplayName("equal sizes")
        class EqualSizes {

            @Test
            @DisplayName("partition to list of lists")
            void testListOfLists() {
                Collector<Integer, ?, List<List<Integer>>> collector = partitioning(10, toList(), toList());
                List<List<Integer>> result = IntStream.range(10, 40)
                        .boxed()
                        .collect(collector);

                List<List<Integer>> expected = Arrays.asList(
                        Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
                        Arrays.asList(20, 21, 22, 23, 24, 25, 26, 27, 28, 29),
                        Arrays.asList(30, 31, 32, 33, 34, 35, 36, 37, 38, 39)
                );

                assertEquals(expected, result);
            }

            @Test
            @DisplayName("partition to list of strings")
            void testListOfStrings() {
                Collector<String, ?, List<String>> collector = partitioning(10, joining(","), toList());
                List<String> result = IntStream.range(10, 40)
                        .mapToObj(Integer::toString)
                        .collect(collector);

                List<String> expected = Arrays.asList(
                        "10,11,12,13,14,15,16,17,18,19",
                        "20,21,22,23,24,25,26,27,28,29",
                        "30,31,32,33,34,35,36,37,38,39"
                );

                assertEquals(expected, result);
            }

            @Test
            @DisplayName("partition to multi-line string")
            void testMultilineString() {
                Collector<String, ?, String> collector = partitioning(10, joining(","), joining("\n"));
                String result = IntStream.range(10, 40)
                        .mapToObj(Integer::toString)
                        .collect(collector);

                String expected = "10,11,12,13,14,15,16,17,18,19\n20,21,22,23,24,25,26,27,28,29\n30,31,32,33,34,35,36,37,38,39";

                assertEquals(expected, result);
            }
        }

        @Nested
        @DisplayName("unequal sizes")
        class UnequalSizes {

            @Test
            @DisplayName("partition to list of lists")
            void testListOfLists() {
                Collector<Integer, ?, List<List<Integer>>> collector = partitioning(10, toList(), toList());
                List<List<Integer>> result = IntStream.range(10, 35)
                        .boxed()
                        .collect(collector);

                List<List<Integer>> expected = Arrays.asList(
                        Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
                        Arrays.asList(20, 21, 22, 23, 24, 25, 26, 27, 28, 29),
                        Arrays.asList(30, 31, 32, 33, 34)
                );

                assertEquals(expected, result);
            }

            @Test
            @DisplayName("partition to list of strings")
            void testListOfStrings() {
                Collector<String, ?, List<String>> collector = partitioning(10, joining(","), toList());
                List<String> result = IntStream.range(10, 35)
                        .mapToObj(Integer::toString)
                        .collect(collector);

                List<String> expected = Arrays.asList(
                        "10,11,12,13,14,15,16,17,18,19",
                        "20,21,22,23,24,25,26,27,28,29",
                        "30,31,32,33,34"
                );

                assertEquals(expected, result);
            }

            @Test
            @DisplayName("partition to multi-line string")
            void testMultilineString() {
                Collector<String, ?, String> collector = partitioning(10, joining(","), joining("\n"));
                String result = IntStream.range(10, 35)
                        .mapToObj(Integer::toString)
                        .collect(collector);

                String expected = "10,11,12,13,14,15,16,17,18,19\n20,21,22,23,24,25,26,27,28,29\n30,31,32,33,34";

                assertEquals(expected, result);
            }
        }

        @Test
        @DisplayName("parallel")
        void testParallel() {
            Collector<Integer, ?, List<List<Integer>>> collector = partitioning(10, toList(), toList());
            Stream<Integer> stream = IntStream.range(10, 40)
                        .parallel()
                        .boxed();

            assertParallelStreamNotSupported(stream, collector);
        }

        @ParameterizedTest
        @ValueSource(ints = { -1, 0 })
        @DisplayName("with invalid partition size")
        void testInvalidPartitionSize(int partitionSize) {
            Collector<Integer, ?, List<Integer>> downstream = toList();
            Collector<List<Integer>, ?, List<List<Integer>>> partitioner = toList();
            assertThrows(IllegalArgumentException.class, () -> partitioning(partitionSize, downstream, partitioner));
        }

        @Test
        @DisplayName("with null downstream collector")
        void testNullDownstreamCollector() {
            Collector<Integer, ?, List<Integer>> partitioner = toList();
            assertThrows(NullPointerException.class, () -> partitioning(1, null, partitioner));
        }

        @Test
        @DisplayName("with null partitioning collector")
        void testNullPartitioningCollector() {
            Collector<Integer, ?, List<Integer>> downstream = toList();
            assertThrows(NullPointerException.class, () -> partitioning(1, downstream, null));
        }
    }

    private <T> void assertParallelStreamNotSupported(Stream<T> stream, Collector<T, ?, ?> collector) {
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> stream.collect(collector));
        // ForkJoinTask may wrap the IllegalStateException in another IllegalStateException
        // If the message is not as expected, assert that its cause is
        if (!exception.getMessage().equals(Messages.AdditionalCollectors.parallelStreamsNotSupported.get())) {
            assertInstanceOf(IllegalStateException.class, exception.getCause());
            assertEquals(Messages.AdditionalCollectors.parallelStreamsNotSupported.get(), exception.getCause().getMessage());
        }
    }
}
