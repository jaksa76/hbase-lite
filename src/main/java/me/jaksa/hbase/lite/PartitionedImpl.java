package me.jaksa.hbase.lite;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Jaksa Vuckovic
 */
public class PartitionedImpl<T> implements Partitioned<T> {
    private final JobBuilder jobBuilder;

    public PartitionedImpl(JobBuilder jobBuilder) {
        this.jobBuilder = jobBuilder;
    }

    @Override
    public <P> Partitioned<T> partitionBy(PartitionFunction<T, P> f) {
        jobBuilder.addPartitioner(f);
        return new PartitionedImpl<T>(jobBuilder);
    }

    @Override
    public <I> Partitioned<I> map(SerializableFunction<T, I> f) {
        jobBuilder.addMapper(f);
        return new PartitionedImpl<I>(jobBuilder);
    }

    @Override
    public <R extends Serializable> Iterable<R> reduce(SerializableFunction<Iterable<T>, R> f) throws IOException {
        jobBuilder.setReducer(f);
        return jobBuilder.reduceToMultipleValues();
    }
}
