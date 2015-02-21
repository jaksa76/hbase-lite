package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

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
     * @throws IOException
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
     * @return
     * @throws IOException
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
     * @param t
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
     * @param key
     * @throws IOException
     */
    public void delete(Object key) throws IOException {
        Delete delete = new Delete(toBytes(key));
        HTable hTable = getHTable();
        hTable.delete(delete);
        hTable.flushCommits();
    }


    public <P> Partitioned<T> partitionBy(Function<T, P> f) {
        return null;
    }


    public <I> Mapped<I> map(Function<T, I> f) {
        return null;
    }


    public <R> R reduce(Function<Iterable<T>, R> f) {
        return null;
    }


    private Scan scan() {
        Scan scan = new Scan();
        for (Column column : columns) {
            scan.addColumn(Bytes.toBytes(column.family), Bytes.toBytes(column.name));
        }
        return scan;
    }

    protected HTable getHTable() throws IOException {
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


    static class Column {
        public final String family;
        public final String name;

        private Column(String family, String name) {
            this.family = family;
            this.name = name;
        }
    }
}
