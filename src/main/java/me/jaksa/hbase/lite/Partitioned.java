package me.jaksa.hbase.lite;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The results that have already been partitioned. They can be further subdivided into smaller partitions.
 *
 * @param <K> the type of key of the partition
 * @param <T> the type of data being subdivided
 *
 * @author Jaksa Vuckovic
 */
public interface Partitioned<K, T> {
    public <P> Partitioned<List, T> partitionBy(PartitionFunction<T, P> f);

    public <I> Partitioned<K, I> map(SerializableFunction<T, I> f);

    // maybe we should return a Map<R> here
    public <R extends Serializable> Map<K, R> reduce(SerializableFunction<Iterable<T>, R> f) throws IOException;
}
