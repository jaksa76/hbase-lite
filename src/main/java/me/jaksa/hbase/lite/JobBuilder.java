package me.jaksa.hbase.lite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * @author Jaksa Vuckovic
 */
class JobBuilder {
    private final HTable sourceTable;
    private final TempStorage tempStorage;
    private final Configuration configuration;
    private final Converter converter;
    private final Scan scan;
    private Job job;
    private List<SerializableFunction> mappers;
    private List<SerializableFunction> partitioners;
    private Function reducer;

    public JobBuilder(HTable sourceTable, TempStorage tempStorage,
                      Configuration configuration, Converter converter, Scan scan) {
        this.sourceTable = sourceTable;
        this.tempStorage = tempStorage;
        this.configuration = configuration;
        this.converter = converter;
        this.scan = scan;
    };

    public void addMapper(SerializableFunction mapper) {
        if (mappers == null) mappers = new ArrayList<>();
        mappers.add(mapper);
    }

    public void addPartitioner(SerializableFunction partitioner) {
        if (partitioners == null) partitioners = new ArrayList<>();
        partitioners.add(partitioner);
    }

    public void setReducer(Function reducer) {
        this.reducer = reducer;
    }

    public Job createJob() throws IOException {
        job = Job.getInstance(configuration);

        job.setJarByClass(getClassForJar());

        TableMapReduceUtil.addDependencyJars(job);

        TempStorage tempStorage = TempStorage.getInstance();
        tempStorage.storeConverter(job, converter);
        tempStorage.storeReducerFunction(job, (Serializable) reducer);

        if (mappers == null) {
            TableMapReduceUtil.initTableMapperJob(sourceTable.getName().getName(),
                    scan, Grouper.class, IntWritable.class, BytesWritable.class, job);
        } else {
            tempStorage.storeMapperFunctions(job, mappers);
            TableMapReduceUtil.initTableMapperJob(sourceTable.getName().getName(),
                    scan, MapperAdaptor.class, IntWritable.class, BytesWritable.class, job);
        }

        if (partitioners == null) {
            TableMapReduceUtil.initTableReducerJob(TempStorage.TABLE_NAME, ReducerAdaptor.class, job);
            job.setNumReduceTasks(1); // only 1 reducer for non partitioned data
        } else {
            // TODO use PartitionedReducerAdaptor
        }

        return job;
    }

    public <R> R reduceToSingleValue() throws IOException {
        try {
            if (job == null) job = createJob();
            boolean success = job.waitForCompletion(true);
            if (!success) throw new IOException("Failed processing " + job.getStatus().getFailureInfo());

            // if there are no rows in the table no result will be stored
            R result = tempStorage.retrieveResult(job);

            return (result != null) ? result : (R) reducer.apply(Collections.emptyList());
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new IOException(e);
        }
    }

    private Class<?> getClassForJar() {
        if (mappers != null) return mappers.get(0).getClass();
        if (reducer != null) return reducer.getClass();
        return JobBuilder.class;
    }
}
