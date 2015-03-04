package me.jaksa.hbase.lite;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.function.Function;

/**
 * @author Jaksa Vuckovic
 */
class MappedImpl<T> implements Mapped<T> {
    private final JobBuilder jobBuilder;

    public MappedImpl(JobBuilder jobBuilder) {
        this.jobBuilder = jobBuilder;
    }

    @Override
    public <P> Partitioned<T> partitionBy(PartitionFunction<T, P> f) {
        jobBuilder.addPartitioner(f);
        return new PartitionedImpl<>(jobBuilder);
    }

    @Override
    public <I> Mapped<I> map(SerializableFunction<T, I> f) {
        jobBuilder.addMapper(f);
        return new MappedImpl<I>(jobBuilder);
    }

    @Override
    public <R> R reduce(SerializableFunction<Iterable<T>, R> f) throws IOException {
        jobBuilder.setReducer(f);
        return jobBuilder.reduceToSingleValue();
    }
}
