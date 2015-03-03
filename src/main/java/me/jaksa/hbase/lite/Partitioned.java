package me.jaksa.hbase.lite;

import java.io.IOException;
import java.io.Serializable;
import java.util.function.Function;

/**
 * The results that have already been partitioned. They can be further subdivided into smaller partitions.
 *
 * @param <T> the type of data being subdivided
 *
 * @author Jaksa Vuckovic
 */
public interface Partitioned<T> {
    public <P> Partitioned<T> partitionBy(PartitionFunction<T, P> f);

    public <I> Partitioned<I> map(SerializableFunction<T, I> f);

    // maybe we should return a Map<R> here
    public <R extends Serializable> Iterable<R> reduce(SerializableFunction<Iterable<T>, R> f) throws IOException;
}