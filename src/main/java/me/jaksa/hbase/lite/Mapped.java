package me.jaksa.hbase.lite;

import java.io.IOException;
import java.util.function.Function;

/**
 * Output from a map function that hasn't been partitioned.
 *
 * @param <T> the type of data
 */
public interface Mapped<T> {
    public <P> Partitioned<T> partitionBy(PartitionFunction<T, P> f);

    public <I> Mapped<I> map(SerializableFunction<T, I> f);

    public <R> R reduce(SerializableFunction<Iterable<T>, R> f) throws IOException;
}
