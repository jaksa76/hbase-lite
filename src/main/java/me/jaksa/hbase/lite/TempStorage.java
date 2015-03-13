package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * Used for storing parameters and results of map reduce jobs.
 *
 * @author Jaksa Vuckovic
 */
class TempStorage {

    public static final String TABLE_NAME = "_hbase-lite-temp";
    public static final byte[] COLUMN_FAMILY = toBytes("cf");
    public static final byte[] VALUE = toBytes("val");
    public static final String REDUCER_KEY = "reducer-key";
    public static final String MAPPERS_KEY = "mappers-key";
    private static TempStorage instance;

    private final HTable hTable;

    public static synchronized TempStorage getInstance() throws IOException {
        if (instance == null) instance = new TempStorage();
        return instance;
    }

    private TempStorage() throws IOException {
        HBaseAdmin hbase = new HBaseAdmin(HBaseLite.getConfiguration());
        if (!hbase.tableExists(TABLE_NAME)) {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            HColumnDescriptor family = new HColumnDescriptor(COLUMN_FAMILY);
            desc.addFamily(family);
            hbase.createTable(desc);
        }
        hTable = new HTable(HBaseLite.getConfiguration(), TABLE_NAME);
    }

    public void storeReducerFunction(Job job, Serializable reducer) throws IOException {
        String reducerKey = Long.toString(System.nanoTime());
        job.getConfiguration().set(REDUCER_KEY, reducerKey);

        Put put = new Put(Bytes.toBytes(reducerKey));
        put.add(COLUMN_FAMILY, VALUE, SerializableUtils.toBytes(reducer));
        hTable.put(put);
    }

    public <T> T loadReducerFunction(Reducer.Context context) throws IOException {
        String reducerKey = context.getConfiguration().get(REDUCER_KEY);
        Get get = new Get(toBytes(reducerKey));
        get.addColumn(COLUMN_FAMILY, VALUE);
        Result results = hTable.get(get);
        try {
            byte[] value = results.getValue(COLUMN_FAMILY, VALUE);
            return (T) SerializableUtils.fromBytes(value);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("could not deserialize reducer", e);
        }
    }

    public <R extends Serializable> void storeResult(Reducer.Context context, R result) throws IOException, InterruptedException {
        String jobId = context.getJobID().getJtIdentifier();
        Text keyout = new Text(jobId);
        Put put = new Put(toBytes(jobId));
        put.add(COLUMN_FAMILY, VALUE, SerializableUtils.toBytes(result));
        context.write(keyout, put);
    }

    public <R extends Serializable> void storeResult(Reducer.Context context, BytesWritable key, R result) throws IOException, InterruptedException {
        String jobId = context.getJobID().getJtIdentifier();
        Text keyout = new Text(jobId);
        Put put = new Put(toBytes(jobId));
        put.add(COLUMN_FAMILY, key.copyBytes(), SerializableUtils.toBytes(result));
        context.write(keyout, put);
    }

    public <R extends Serializable> R retrieveResult(Job job) throws IOException, ClassNotFoundException {
        Get get = new Get(toBytes(job.getJobID().getJtIdentifier()));
        get.addColumn(COLUMN_FAMILY, VALUE);
        Result results = hTable.get(get);
        if (results.isEmpty()) return null;
        byte[] value = results.getValue(COLUMN_FAMILY, VALUE);
        return (R) SerializableUtils.fromBytes(value);
    }

    public <R extends Serializable> Iterable<R> retrieveResults(Job job) throws IOException, ClassNotFoundException {
        Get get = new Get(toBytes(job.getJobID().getJtIdentifier()));
        Result row = hTable.get(get);
        if (row.isEmpty()) return null;

        NavigableMap<byte[], byte[]> familyMap = row.getFamilyMap(COLUMN_FAMILY);

        ArrayList<R> results = new ArrayList<>(familyMap.size());
        for (byte[] value : familyMap.values()) {
            results.add((R) SerializableUtils.fromBytes(value));
        };

        return results;
    }

    public <T> Converter<T> retrieveConverter(Mapper.Context context) throws IOException {
        Class<Converter<T>> converterClazz = (Class<Converter<T>>) context.getConfiguration().getClass("converter", Converter.class);
        Class<T> tClass = (Class<T>) context.getConfiguration().getClass("element", Converter.class);
        try {
            Converter<T> converter = converterClazz.newInstance();
            if (converter instanceof GenericConverter) ((GenericConverter) converter).setElementClass(tClass);
            return converter;
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IOException("the converter class must have a no-arg public constructor", e);
        }
    }

    public void storeConverter(Job job, Converter converter) {
        job.getConfiguration().setClass("converter", converter.getClass(), Converter.class);
    }

    public void storeElementClass(Job job, GenericConverter converter) {
        job.getConfiguration().setClass("element", converter.getElementClass(), Serializable.class);
    }

    public void storeMapperFunctions(Job job, List<SerializableFunction> mappers) throws IOException {
        String reducerKey = Long.toString(System.nanoTime());
        job.getConfiguration().set(MAPPERS_KEY, reducerKey);

        Put put = new Put(Bytes.toBytes(reducerKey));
        put.add(COLUMN_FAMILY, VALUE, SerializableUtils.toBytes((Serializable) mappers));
        hTable.put(put);
    }

    public List<SerializableFunction> loadMapperFunctions(Mapper.Context context) throws IOException {
        String reducerKey = context.getConfiguration().get(MAPPERS_KEY);
        Get get = new Get(toBytes(reducerKey));
        get.addColumn(COLUMN_FAMILY, VALUE);
        Result results = hTable.get(get);
        try {
            byte[] value = results.getValue(COLUMN_FAMILY, VALUE);
            return (List<SerializableFunction>) SerializableUtils.fromBytes(value);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("could not deserialize mappers", e);
        }
    }
}
