package me.jaksa.hbase.lite;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author Jaksa Vuckovic
 */
class PartitionedImpl<K, T> implements Partitioned<K, T> {
    private final JobBuilder jobBuilder;

    public PartitionedImpl(JobBuilder jobBuilder) {
        this.jobBuilder = jobBuilder;
    }

    @Override
    public <P> Partitioned<List, T> partitionBy(PartitionFunction<T, P> f) {
        jobBuilder.addPartitioner(f);
        return new PartitionedImpl<List, T>(jobBuilder);
    }

    @Override
    public <I> Partitioned<K, I> map(SerializableFunction<T, I> f) {
        jobBuilder.addMapper(f);
        return new PartitionedImpl<K, I>(jobBuilder);
    }

    @Override
    public <R extends Serializable> Map<K, R> reduce(SerializableFunction<Iterable<T>, R> f) throws IOException {
        jobBuilder.setReducer(f);
        return jobBuilder.reduceToMultipleValues();
    }
}
