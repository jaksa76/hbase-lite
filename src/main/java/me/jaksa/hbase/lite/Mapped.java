package me.jaksa.hbase.lite;

import java.util.function.Function;

public interface Mapped<T> {
    public <P> Partitioned<T> partitionBy(Function<T, P> f);

    public <I> Mapped<I> map(Function<T, I> f);

    public <R> R reduce(Function<Iterable<T>, R> f);
}
