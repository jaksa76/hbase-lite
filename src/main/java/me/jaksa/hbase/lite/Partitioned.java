package me.jaksa.hbase.lite;

import java.util.function.Function;

public interface Partitioned<T> {
    public <P> Partitioned<T> partitionBy(Function<T, P> f);

    public <I> Partitioned<I> map(Function<T, I> f);

    // maybe we should return a Map<R> here
    public <R> Iterable<R> reduce(Function<Iterable<T>, R> f);
}
