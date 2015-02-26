package me.jaksa.hbase.lite;

import java.util.function.Function;

/**
 * Output from a map function that hasn't been partitioned.
 *
 * @param <T> the type of data
 */
public interface Mapped<T> {
    public <P> Partitioned<T> partitionBy(Function<T, P> f);

    public <I> Mapped<I> map(Function<T, I> f);

    public <R> R reduce(Function<Iterable<T>, R> f);
}
