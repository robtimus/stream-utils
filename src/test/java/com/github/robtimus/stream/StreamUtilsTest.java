/*
 * StreamUtilsTest.java
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
import static com.github.robtimus.stream.StreamUtils.forEvery;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StreamUtilsTest {

    @Nested
    @DisplayName("forEvery")
    class ForEvery {

        @Test
        @DisplayName("empty stream")
        void testEmptyStream() {
            Stream<Integer> stream = Stream.empty();
            @SuppressWarnings("unchecked")
            Consumer<List<Integer>> action = mock(Consumer.class);

            forEvery(1, stream, toList(), action);

            verifyNoInteractions(action);
        }

        @Test
        @DisplayName("equal sizes")
        void testEqualSizes() {
            Stream<Integer> stream = IntStream.range(10, 40)
                    .boxed();
            @SuppressWarnings("unchecked")
            Consumer<List<Integer>> action = mock(Consumer.class);

            forEvery(10, stream, toList(), action);

            verify(action).accept(Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
            verify(action).accept(Arrays.asList(20, 21, 22, 23, 24, 25, 26, 27, 28, 29));
            verify(action).accept(Arrays.asList(30, 31, 32, 33, 34, 35, 36, 37, 38, 39));
            verifyNoMoreInteractions(action);
        }

        @Test
        @DisplayName("unequal sizes")
        void testUnequalSizes() {
            Stream<Integer> stream = IntStream.range(10, 35)
                    .boxed();
            @SuppressWarnings("unchecked")
            Consumer<List<Integer>> action = mock(Consumer.class);

            forEvery(10, stream, toList(), action);

            verify(action).accept(Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19));
            verify(action).accept(Arrays.asList(20, 21, 22, 23, 24, 25, 26, 27, 28, 29));
            verify(action).accept(Arrays.asList(30, 31, 32, 33, 34));
            verifyNoMoreInteractions(action);
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
        @DisplayName("with invalid count")
        void testInvalidCount(int count) {
            Stream<Integer> stream = Stream.empty();
            Collector<Integer, ?, List<Integer>> collector = toList();
            @SuppressWarnings("unchecked")
            Consumer<List<Integer>> action = mock(Consumer.class);

            assertThrows(IllegalArgumentException.class, () -> forEvery(count, stream, collector, action));
        }

        @Test
        @DisplayName("null stream")
        void testNullStream() {
            Collector<Integer, ?, List<Integer>> collector = toList();
            @SuppressWarnings("unchecked")
            Consumer<List<Integer>> action = mock(Consumer.class);

            assertThrows(NullPointerException.class, () -> forEvery(1, null, collector, action));
        }

        @Test
        @DisplayName("null collector")
        void testNullCollector() {
            Stream<Integer> stream = Stream.empty();
            @SuppressWarnings("unchecked")
            Consumer<List<Integer>> action = mock(Consumer.class);

            assertThrows(NullPointerException.class, () -> forEvery(1, stream, null, action));
        }

        @Test
        @DisplayName("null action")
        void testNullAction() {
            Stream<Integer> stream = Stream.empty();
            Collector<Integer, ?, List<Integer>> collector = toList();

            assertThrows(NullPointerException.class, () -> forEvery(1, stream, collector, null));
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
