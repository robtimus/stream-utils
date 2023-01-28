# stream-utils
[![Maven Central](https://img.shields.io/maven-central/v/com.github.robtimus/stream-utils)](https://search.maven.org/artifact/com.github.robtimus/stream-utils)
[![Build Status](https://github.com/robtimus/stream-utils/actions/workflows/build.yml/badge.svg)](https://github.com/robtimus/stream-utils/actions/workflows/build.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.github.robtimus%3Astream-utils&metric=alert_status)](https://sonarcloud.io/summary/overall?id=com.github.robtimus%3Astream-utils)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.github.robtimus%3Astream-utils&metric=coverage)](https://sonarcloud.io/summary/overall?id=com.github.robtimus%3Astream-utils)
[![Known Vulnerabilities](https://snyk.io/test/github/robtimus/stream-utils/badge.svg)](https://snyk.io/test/github/robtimus/stream-utils)

Provides utility classes for working with streams.

## Additional collectors

Class [AdditionalCollectors](https://robtimus.github.io/stream-utils/apidocs/com/github/robtimus/stream/AdditionalCollectors.html) comes with the following utility methods:

### sequentialOnly
`sequentialOnly` is a collector factory method similar to `Collector.of`, but without the combiner. This can be used for collectors that cannot easily be combined, in cases where only sequential streams are used.

### findSingle and findUnique
`findSingle` is like `Stream.findAny`, but it throws an exception if the stream contains more than one element. It can be combined with `Optional.orElseThrow` to find exactly one element. Note that unlike `Stream.findAny`, the entire stream is processed.

`findUnique` is like `findSingle` that allows multiple occurrences of the same element.

### toMapWithSupplier
`toMapWithSupplier` is like `Collectors.toMap`. However, unlike the version with only a key and value mapper, it allows you to provide a `Map` supplier. Unlike the version with a supplier there is no need to provide a merge function.

### completionStages and completableFutures
`completableFutures` can collect a stream of `CompletableFuture` elements into a combined `CompletableFuture`.
`completionStages` is a more generic version of `completableFutures` that can collect a stream of `CompletionStage` elements. It can be used wherever `completableFutures` can be used.

### partitioning
`partitioning` splits the stream into several partitions. Each partition is collected separately, and these partition results are then collected. For example, the following can be used to create a `List<List<T>>`, where each inner list has at most 10 elements:

    stream.collect(partitioning(10, toList(), toList())

Because it's hard to get predictable results for parallel streams, this collector will throw an exception when used to collect parallel streams.

## Stream utils

Class [StreamUtils](https://robtimus.github.io/stream-utils/apidocs/com/github/robtimus/stream/StreamUtils.html) comes with the following utility methods:

### forEvery

`forEvery` is like `AdditionalCollectors.partitioning` (see above), but runs an action for each partition result instead of collecting them. For example, to print 10 elements per line:

    forEvery(10, stream, joining(", "), System.out::println);

Like `AdditionalCollectors.partitioning`, this method will throw an exception when used to collect parallel streams.

## FutureValue

Class [FutureValue](https://robtimus.github.io/stream-utils/apidocs/com/github/robtimus/stream/FutureValue.html) provides utility methods to work with `CompletableFuture` in streams. Unlike `AdditionalCollectors.completableFutures`, methods in this class can be used to provide intermediate filtering and mapping. It also provides support for `Stream.collect` and `Stream.forEach` for `CompletableFuture`.

To use this class, use `Stream.map` in combination with `FutureValue.wrap`. Then `FutureValue.filter`, `FutureValue.map` and `FutureValue.flatMap` can be used any number of times before `Stream.collect` or `Stream.forEach` is called.

For example:

    // assume stream is an existing Stream<CompletableFuture<T>>
    CompletableFuture<List<T>> list = stream
            .map(FutureValue::wrap)
            .map(FutureValue.filter(Objects::nonNull))
            .collect(FutureValue.collect(toList()));
