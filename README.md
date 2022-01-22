# stream-utils

Provides utility classes for working with streams.

## Additional collectors

Class [AdditionalCollectors](https://robtimus.github.io/stream-utils/apidocs/com/github/robtimus/stream/AdditionalCollectors.html) comes with the following utility methods:

### sequentialOnly
`sequentialOnly` is a collector factory method similar to `Collector.of`, but without the combiner. This can be used for collectors that cannot easily be combined, in cases where only sequential streams are used.

### findSingle and findUnique
`findSingle` is like `Stream.findAny`, but it throws an exception if the stream contains more than one element. It can be combined with `Optional.orElseThrow` to find exactly one element.

`findUnique` is like `findSingle` that allows multiple occurrences of the same element.

### toMapWithSupplier
`toMapWithSupplier` is like `Collectors.toMap`. However, unlike the version with only a key and value mapper, it allows you to provide a `Map` supplier. Unlike the version with a supplier there is no need to provide a merge function.

### completableFutures
`completableFutures` can collect a stream of `CompletableFuture` elements into a combined `CompletableFuture`. Since the `CompletableFuture` results are not available until collection time, additional method `filtering` can be used to provide filtering during collection time.

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

Class [FutureValue](https://robtimus.github.io/stream-utils/apidocs/com/github/robtimus/stream/FutureValue.html) provides utility methods to work with `CompletableFuture` in streams. Unlike `AdditionalCollectors.completableFutures`, methods in this class can be used to provide intermediate filtering and mapping. It also provides support for `Stream.reduce` and `Stream.forEach` for `CompletableFuture`.

To use this class, use `Stream.map` in combination with `FutureValue.wrap`. Then `FutureValue.filter` and `FutureValue.map` can be used any number of times before `Stream.reduce`, `Stream.collect` or `Stream.forEach` is called.

Some examples:

    // assume stream is an existing Stream<CompletableFuture<Integer>>
    CompletableFuture<Integer> result = stream
            .map(FutureValue::wrap)
            .map(FutureValue.filter(i -> (i & 1) == 0))
            .map(FutureValue.map(i -> i * i))
            .reduce(FutureValue.identity(0), FutureValue.accumulate(Integer::sum))
            .asFuture();

    // assume stream is an existing Stream<CompletableFuture<Integer>>
    Optional<CompletableFuture<Integer>> result = stream
            .map(FutureValue::wrap)
            .map(FutureValue.filter(i -> (i & 1) == 0))
            .map(FutureValue.map(i -> i * i))
            .reduce(FutureValue.accumulate(Integer::sum))
            .map(FutureValue::asFuture);

    // assume stream is an existing Stream<CompletableFuture<T>>
    CompletableFuture<List<T>> list = stream
            .map(FutureValue::wrap)
            .map(FutureValue.filter(Objects::nonNull))
            .collect(FutureValue.collect(toList()));

Note that this last example is equivalent to the following:

    CompletableFuture<List<T>> list = stream
            .collect(filtering(completableFutures(toList()), Objects::nonNull));
