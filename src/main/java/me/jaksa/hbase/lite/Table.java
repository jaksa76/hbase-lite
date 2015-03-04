package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an HBase table for a specific type of domain object.
 *
 * You can have
 * multiple Tables defined for the same table in HBase each having a different set of columns
 * and a different domain object. In that case storing an object in one table will not
 * overwrite columns that are not defined for that table.
 *
 * @author Jaksa Vuckovic
 */
public class Table<T> {
    private final String name;
    private final List<Column> columns;
    private final Converter<T> converter;
    private HTable hTable;


    /**
     * @param name the name of the table
     * @param columns a comma separated list of columns (e.g. "fam1:col1,fam1:col2,fam2:col1")
     * @param converter the converter for the domain objects
     */
    public Table(String name, String columns, Converter<T> converter) {
        this.name = name;
        this.converter = converter;
        this.columns = extractColumns(columns);
    }


    public Table(HTable hTable, String columns, Converter<T> converter) {
        this(hTable.getName().getNameAsString(), columns, converter);
        this.hTable = hTable;
    }


    /**
     * Retrieve the object with the specified key. This method will transform the key to a byte array before invoking
     * HBase.
     *
     * @param key the key of this object
     * @return the object requested
     * @throws java.io.IOException if there is a communication problem with HBase
     */
    public T get(Object key) throws IOException {
        Get get = new Get(toBytes(key));
        for (Column column : columns) {
            get.addColumn(Bytes.toBytes(column.family), Bytes.toBytes(column.name));
        }
        Result result = getHTable().get(get);
        if (result == null || result.isEmpty()) return null;
        return converter.convert(result);
    }


    /**
     * Store the object into HBase.
     *
     * @param t the object to store
     * @throws java.io.IOException if there is a communication problem with HBase
     */
    public void put(T t) throws IOException {
        HTable hTable = getHTable();
        hTable.put(converter.toPut(t));
        hTable.flushCommits();
    }

    /**
     * Deletes the object with the specified key. This method will transform the key to a byte array before invoking
     * HBase.
     *
     * @param key the key of the object to delete
     * @throws java.io.IOException if there is a communication problem with HBase
     */
    public void delete(Object key) throws IOException {
        Delete delete = new Delete(toBytes(key));
        HTable hTable = getHTable();
        hTable.delete(delete);
        hTable.flushCommits();
    }

    /**
     * Partition the data according to a partitioning function. The function should return
     * the partition id. All data for which the function produces the same key will end up
     * in the same parititon.
     *
     * @param f the function that determines the partition
     * @param <P> type of partition id
     * @return data that can be processed further
     * @throws java.io.IOException if there is a communication problem with HBase
     */
    public <P> Partitioned<T> partitionBy(PartitionFunction<T, P> f) throws IOException {
        JobBuilder jobBuilder = createJobBuilder();
        jobBuilder.addPartitioner(f);
        return new PartitionedImpl<T>(jobBuilder);
    }


    /**
     * Transform the data using the given function.
     *
     * @param f the function to transform the data
     * @param <I> the type of the output data
     * @return the data resulting from applying the function
     * @throws java.io.IOException if there is a communication problem with HBase
     */
    public <I> Mapped<I> map(SerializableFunction<T, I> f) throws IOException {
        JobBuilder jobBuilder = createJobBuilder();
        jobBuilder.addMapper(f);
        return new MappedImpl<>(jobBuilder);
    }

    /**
     * Applies a function over all the data. The function receives an Iterable
     * for the data in the table and should produce a result.
     *
     * @param f the function to be applied.
     * @param <R> the type of the result
     * @return the result of the function
     * @throws java.io.IOException if there is a communication problem with HBase
     */
    public <R extends Serializable> R reduce(SerializableFunction<Iterable<T>, R> f) throws IOException {
        JobBuilder jobBuilder = createJobBuilder();
        jobBuilder.setReducer(f);
        return jobBuilder.reduceToSingleValue();
    }


    private JobBuilder createJobBuilder() throws IOException {TempStorage tempStorage = TempStorage.getInstance();
        return new JobBuilder(hTable, tempStorage, HBaseLite.getConfiguration(), converter, scan());
    }


    private Scan scan() {
        Scan scan = new Scan();
        for (Column column : columns) {
            scan.addColumn(Bytes.toBytes(column.family), Bytes.toBytes(column.name));
        }
        return scan;
    }


    private HTable getHTable() throws IOException {
        // we use lazy initialization in case we'll want to serialize this class at some point
        if (hTable == null) hTable = new HTable(HBaseLite.getConfiguration(), name);
        return hTable;
    }


    static List<Column> extractColumns(String columns) {
        List<Column> columnsList = new ArrayList<Column>();
        if (columns == null || columns.isEmpty()) throw new IllegalArgumentException("you must specify some columns");
        for (String columnString : columns.split("\\s*,\\s*")) {
            String[] columnNameParts = columnString.split("\\s*:\\s*");
            if (columnNameParts.length != 2) throw new IllegalArgumentException("no valid family name in " + columnString);
            String family = columnNameParts[0];
            String columnName = columnNameParts[1];
            columnsList.add(new Column(family, columnName));
        }
        return columnsList;
    }


    static byte[] toBytes(Object key) {
        if (key instanceof String) return Bytes.toBytes((String) key);
        if (key instanceof Integer) return Bytes.toBytes((Integer) key);
        if (key instanceof Long) return Bytes.toBytes((Long) key);
        if (key instanceof BigDecimal) return Bytes.toBytes((BigDecimal) key);
        if (key instanceof ByteBuffer) return Bytes.toBytes((ByteBuffer) key);
        if (key instanceof Double) return Bytes.toBytes((Double) key);
        if (key instanceof Float) return Bytes.toBytes((Float) key);
        if (key instanceof Short) return Bytes.toBytes((Short) key);
        if (key instanceof Boolean) return Bytes.toBytes((Boolean) key);
        throw new IllegalArgumentException("HBase doesn't support keys of type " + key.getClass().getName());
    }

    public void deleteAll() throws IOException {
        List<Delete> deletes = new ArrayList<>();
        ResultScanner scanner = getHTable().getScanner(scan());
        for (Result result : scanner) {
            Delete delete = new Delete(result.getRow());
            deletes.add(delete);
        }
        scanner.close();
        hTable.delete(deletes);
    }


    static class Column {
        public final String family;
        public final String name;

        private Column(String family, String name) {
            this.family = family;
            this.name = name;
        }
    }
}
