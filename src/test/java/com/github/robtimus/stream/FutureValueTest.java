/*
 * FutureValueTest.java
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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FutureValueTest {

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

    @Nested
    @DisplayName("wrap")
    class Wrap {

        @Test
        @DisplayName("null future")
        void testNullFuture() {
            assertThrows(NullPointerException.class, () -> FutureValue.wrap(null));
        }
    }

    @Nested
    @DisplayName("filter")
    class Filter {

        @Test
        @DisplayName("filtering once")
        void testFilteringOnce() {
            int size = 2 * threadPoolSize;

            CompletableFuture<List<Integer>> result = IntStream.range(0, size)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                    .map(FutureValue::wrap)
                    .map(FutureValue.filter(i -> (i & 1) == 0))
                    .collect(FutureValue.collect(toList()));

            List<Integer> expected = IntStream.range(0, size)
                    .map(i -> i * i)
                    .filter(i -> (i & 1) == 0)
                    .boxed()
                    .collect(toList());

            List<Integer> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

            assertEquals(expected, resultList);
        }

        @Test
        @DisplayName("filtering twice")
        void testFilteringTwice() {
            int size = 2 * threadPoolSize;

            CompletableFuture<List<Integer>> result = IntStream.range(0, size)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                    .map(FutureValue::wrap)
                    .map(FutureValue.filter(i -> (i & 1) == 0))
                    .map(FutureValue.filter(i -> {
                        // assert that elements that don't match the first predicate are not filtered again
                        assertEquals(0, i & 1);
                        return (i & 2) == 0;
                    }))
                    .collect(FutureValue.collect(toList()));

            List<Integer> expected = IntStream.range(0, size)
                    .map(i -> i * i)
                    .filter(i -> (i & 3) == 0)
                    .boxed()
                    .collect(toList());

            List<Integer> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

            assertEquals(expected, resultList);
        }

        @Test
        @DisplayName("null predicate")
        void testNullPredicate() {
            assertThrows(NullPointerException.class, () -> FutureValue.filter(null));
        }
    }

    @Nested
    @DisplayName("map")
    class Map {

        @Test
        @DisplayName("unfiltered")
        void testUnfiltered() {
            int size = 2 * threadPoolSize;

            CompletableFuture<List<String>> result = IntStream.range(0, size)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                    .map(FutureValue::wrap)
                    .map(FutureValue.map(String::valueOf))
                    .collect(FutureValue.collect(toList()));

            List<String> expected = IntStream.range(0, size)
                    .map(i -> i * i)
                    .mapToObj(String::valueOf)
                    .collect(toList());

            List<String> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

            assertEquals(expected, resultList);
        }

        @Test
        @DisplayName("filtered")
        void testFiltered() {
            int size = 2 * threadPoolSize;

            CompletableFuture<List<String>> result = IntStream.range(0, size)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                    .map(FutureValue::wrap)
                    .map(FutureValue.filter(i -> (i & 1) == 0))
                    .map(FutureValue.map(i -> {
                        // assert that elements that don't match the predicate are not mapped
                        assertEquals(0, i & 1);
                        return String.valueOf(i);
                    }))
                    .collect(FutureValue.collect(toList()));

            List<String> expected = IntStream.range(0, size)
                    .map(i -> i * i)
                    .filter(i -> (i & 1) == 0)
                    .mapToObj(String::valueOf)
                    .collect(toList());

            List<String> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

            assertEquals(expected, resultList);
        }

        @Test
        @DisplayName("null mapper")
        void testNullPredicate() {
            assertThrows(NullPointerException.class, () -> FutureValue.map(null));
        }
    }

    @Nested
    @DisplayName("accumulate")
    class Accumulate {

        @Test
        @DisplayName("first filtered")
        void testFirstFiltered() {
            FutureValue<Integer> value1 = FutureValue.wrap(CompletableFuture.supplyAsync(() -> calculate(1), executor));
            FutureValue<Integer> value2 = FutureValue.wrap(CompletableFuture.supplyAsync(() -> calculate(2), executor));

            value1 = FutureValue.filter((Integer i) -> false).apply(value1);

            FutureValue<Integer> result = FutureValue.accumulate(Integer::sum).apply(value1, value2);

            Integer resultValue = assertDoesNotThrow(() -> result.asFuture().get(10, TimeUnit.SECONDS));

            assertEquals(2 * 2, resultValue);
        }

        @Test
        @DisplayName("second filtered")
        void testSecondFiltered() {
            FutureValue<Integer> value1 = FutureValue.wrap(CompletableFuture.supplyAsync(() -> calculate(1), executor));
            FutureValue<Integer> value2 = FutureValue.wrap(CompletableFuture.supplyAsync(() -> calculate(2), executor));

            value2 = FutureValue.filter((Integer i) -> false).apply(value2);

            FutureValue<Integer> result = FutureValue.accumulate(Integer::sum).apply(value1, value2);

            Integer resultValue = assertDoesNotThrow(() -> result.asFuture().get(10, TimeUnit.SECONDS));

            assertEquals(1 * 1, resultValue);
        }

        @Test
        @DisplayName("both filtered")
        void testBothFiltered() {
            FutureValue<Integer> value1 = FutureValue.wrap(CompletableFuture.supplyAsync(() -> calculate(1), executor));
            FutureValue<Integer> value2 = FutureValue.wrap(CompletableFuture.supplyAsync(() -> calculate(2), executor));

            value1 = FutureValue.filter((Integer i) -> false).apply(value1);
            value2 = FutureValue.filter((Integer i) -> false).apply(value2);

            FutureValue<Integer> result = FutureValue.accumulate(Integer::sum).apply(value1, value2);
            CompletableFuture<Integer> future = result.asFuture();

            ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, exception.getCause());
        }

        @Test
        @DisplayName("neither filtered")
        void testNeitherFiltered() {
            FutureValue<Integer> value1 = FutureValue.wrap(CompletableFuture.supplyAsync(() -> calculate(1), executor));
            FutureValue<Integer> value2 = FutureValue.wrap(CompletableFuture.supplyAsync(() -> calculate(2), executor));

            BinaryOperator<FutureValue<Integer>> accumulator = FutureValue.accumulate(Integer::sum);

            FutureValue<Integer> result = accumulator.apply(value1, value2);

            Integer resultValue = assertDoesNotThrow(() -> result.asFuture().get(10, TimeUnit.SECONDS));

            assertEquals(1 * 1 + 2 * 2, resultValue);
        }

        @Test
        @DisplayName("null accumulator")
        void testNullPredicate() {
            assertThrows(NullPointerException.class, () -> FutureValue.accumulate(null));
        }
    }

    @Nested
    @DisplayName("reduce")
    class Reduce {

        @Nested
        @DisplayName("without identity")
        class WithoutIdentity {

            @Test
            @DisplayName("empty")
            void testEmpty() {
                int size = 0;

                Optional<CompletableFuture<Integer>> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .map(FutureValue.filter(i -> (i & 1) == 0))
                        .reduce(FutureValue.accumulate(Integer::sum))
                        .map(FutureValue::asFuture);

                assertEquals(Optional.empty(), result);
            }

            @Test
            @DisplayName("evens")
            void testReduceEvens() {
                int size = 2 * threadPoolSize;

                Optional<CompletableFuture<Integer>> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .map(FutureValue.filter(i -> (i & 1) == 0))
                        .reduce(FutureValue.accumulate(Integer::sum))
                        .map(FutureValue::asFuture);

                int expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .filter(i -> (i & 1) == 0)
                        .reduce(0, Integer::sum);

                assertNotEquals(Optional.empty(), result);

                Integer resultValue = assertDoesNotThrow(() -> result.get().get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultValue);
            }

            @Test
            @DisplayName("odds")
            void testReduceOdds() {
                int size = 2 * threadPoolSize;

                Optional<CompletableFuture<Integer>> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .map(FutureValue.filter(i -> (i & 1) == 1))
                        .reduce(FutureValue.accumulate(Integer::sum))
                        .map(FutureValue::asFuture);

                int expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .filter(i -> (i & 1) == 1)
                        .reduce(0, Integer::sum);

                assertNotEquals(Optional.empty(), result);

                Integer resultValue = assertDoesNotThrow(() -> result.get().get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultValue);
            }

            @Test
            @DisplayName("all")
            void testReduceAll() {
                int size = 2 * threadPoolSize;

                Optional<CompletableFuture<Integer>> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .reduce(FutureValue.accumulate(Integer::sum))
                        .map(FutureValue::asFuture);

                int expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .reduce(0, Integer::sum);

                Integer resultValue = assertDoesNotThrow(() -> result.get().get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultValue);
            }
        }

        @Nested
        @DisplayName("with identity")
        class WithIdentity {

            @Test
            @DisplayName("empty")
            void testEmpty() {
                int size = 0;

                CompletableFuture<Integer> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .map(FutureValue.filter(i -> (i & 1) == 0))
                        .reduce(FutureValue.identity(0), FutureValue.accumulate(Integer::sum))
                        .asFuture();

                int expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .filter(i -> (i & 1) == 0)
                        .reduce(0, Integer::sum);

                Integer resultValue = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultValue);
            }

            @Test
            @DisplayName("evens")
            void testReduceEvens() {
                int size = 2 * threadPoolSize;

                CompletableFuture<Integer> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .map(FutureValue.filter(i -> (i & 1) == 0))
                        .reduce(FutureValue.identity(0), FutureValue.accumulate(Integer::sum))
                        .asFuture();

                int expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .filter(i -> (i & 1) == 0)
                        .reduce(0, Integer::sum);

                Integer resultValue = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultValue);
            }

            @Test
            @DisplayName("odds")
            void testReduceOdds() {
                int size = 2 * threadPoolSize;

                CompletableFuture<Integer> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .map(FutureValue.filter(i -> (i & 1) == 1))
                        .reduce(FutureValue.identity(0), FutureValue.accumulate(Integer::sum))
                        .asFuture();

                int expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .filter(i -> (i & 1) == 1)
                        .reduce(0, Integer::sum);

                Integer resultValue = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultValue);
            }

            @Test
            @DisplayName("all")
            void testReduceAll() {
                int size = 2 * threadPoolSize;

                CompletableFuture<Integer> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .reduce(FutureValue.identity(0), FutureValue.accumulate(Integer::sum))
                        .asFuture();

                int expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .reduce(0, Integer::sum);

                Integer resultValue = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultValue);
            }
        }
    }

    @Nested
    @DisplayName("collect")
    class Collect {

        @Nested
        @DisplayName("sequential")
        class Sequential {

            @Test
            @DisplayName("unfiltered")
            void testUnfiltered() {
                int size = 2 * threadPoolSize;

                CompletableFuture<List<Integer>> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .collect(FutureValue.collect(toList()));

                List<Integer> expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .boxed()
                        .collect(toList());

                List<Integer> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultList);
            }

            @Test
            @DisplayName("filtered")
            void testFiltered() {
                int size = 2 * threadPoolSize;

                CompletableFuture<List<Integer>> result = IntStream.range(0, size)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .map(FutureValue.filter(i -> (i & 1) == 0))
                        .collect(FutureValue.collect(toList()));

                List<Integer> expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .filter(i -> (i & 1) == 0)
                        .boxed()
                        .collect(toList());

                List<Integer> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultList);
            }
        }

        @Nested
        @DisplayName("parallel")
        class Parallel {

            @Test
            @DisplayName("unfiltered")
            void testUnfiltered() {
                int size = 2 * threadPoolSize;

                CompletableFuture<List<Integer>> result = IntStream.range(0, size)
                        .parallel()
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .collect(FutureValue.collect(toList()));

                List<Integer> expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .boxed()
                        .collect(toList());

                List<Integer> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultList);
            }

            @Test
            @DisplayName("filtered")
            void testFiltered() {
                int size = 2 * threadPoolSize;

                CompletableFuture<List<Integer>> result = IntStream.range(0, size)
                        .parallel()
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                        .map(FutureValue::wrap)
                        .map(FutureValue.filter(i -> (i & 1) == 0))
                        .collect(FutureValue.collect(toList()));

                List<Integer> expected = IntStream.range(0, size)
                        .map(i -> i * i)
                        .filter(i -> (i & 1) == 0)
                        .boxed()
                        .collect(toList());

                List<Integer> resultList = assertDoesNotThrow(() -> result.get(10, TimeUnit.SECONDS));

                assertEquals(expected, resultList);
            }
        }

        @Test
        @DisplayName("null collector")
        void testNullPredicate() {
            assertThrows(NullPointerException.class, () -> FutureValue.collect(null));
        }
    }

    @Nested
    @DisplayName("run")
    class Run {

        @Test
        @DisplayName("unfiltered")
        void testUnfiltered() {
            int size = 2 * threadPoolSize;

            Set<Integer> values = Collections.synchronizedSet(new HashSet<>());

            CountDownLatch latch = new CountDownLatch(size);

            IntStream.range(0, size)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                    .map(FutureValue::wrap)
                    .forEach(FutureValue.run(i -> {
                        values.add(i);
                        latch.countDown();
                    }));

            Set<Integer> expected = IntStream.range(0, size)
                    .map(i -> i * i)
                    .boxed()
                    .collect(toSet());

            boolean countIsZero = assertDoesNotThrow(() -> latch.await(10, TimeUnit.SECONDS));
            assertTrue(countIsZero);

            assertEquals(expected, values);
        }

        @Test
        @DisplayName("filtered")
        void testFiltered() {
            int size = 2 * threadPoolSize;

            Set<Integer> values = Collections.synchronizedSet(new HashSet<>());

            // half of the elements are filtered out, and therefore will never result in a call to latch.countDown()
            CountDownLatch latch = new CountDownLatch(size / 2);

            IntStream.range(0, size)
                    .mapToObj(i -> CompletableFuture.supplyAsync(() -> calculate(i), executor))
                    .map(FutureValue::wrap)
                    .map(FutureValue.filter(i -> (i & 1) == 0))
                    .forEach(FutureValue.run(i -> {
                        // assert that elements that don't match the predicate are not mapped
                        assertEquals(0, i & 1);
                        values.add(i);
                        latch.countDown();
                    }));

            Set<Integer> expected = IntStream.range(0, size)
                    .map(i -> i * i)
                    .filter(i -> (i & 1) == 0)
                    .boxed()
                    .collect(toSet());

            boolean countIsZero = assertDoesNotThrow(() -> latch.await(10, TimeUnit.SECONDS));
            assertTrue(countIsZero);

            assertEquals(expected, values);
        }

        @Test
        @DisplayName("null action")
        void testNullPredicate() {
            assertThrows(NullPointerException.class, () -> FutureValue.run(null));
        }
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
}
