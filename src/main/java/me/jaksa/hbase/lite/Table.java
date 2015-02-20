package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
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
    public Table(String name, String columns, Converter<T> converter) throws IOException {
        this.name = name;
        this.converter = converter;
        List<Column> columnsList = extractColumns(columns);
        this.columns = columnsList;

    }

    protected HTable getHTable() throws IOException {
        if (hTable == null) hTable = new HTable(HBaseConfiguration.create(), name);
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

    /**
     * Retrieve the object with the specified key. This method will transform the key to a byte array before invokeing
     * HBase.
     *
     * @param key the key of this object
     * @return
     * @throws IOException
     */
    public T get(Object key) throws IOException {
        Get get1 = new Get(toBytes(key));
        for (Column column : columns) {
            get1.addColumn(Bytes.toBytes(column.family), Bytes.toBytes(column.name));
        }
        Get get = get1;
        Result result = getHTable().get(get);
        return converter.convert(result);
    }

    /**
     * Store the object into HBase.
     *
     * @param t
     */
    public void put(T t) throws IOException {
        getHTable().put(converter.toPut(t));
    }

    private Scan scan() {
        Scan scan = new Scan();
        for (Column column : columns) {
            scan.addColumn(Bytes.toBytes(column.family), Bytes.toBytes(column.name));
        }
        return scan;
    }

    static byte[] toBytes(Object key) {
        if (key instanceof String) return Bytes.toBytes((String) key);
        if (key instanceof Integer) return Bytes.toBytes((Integer) key);
        if (key instanceof Boolean) return Bytes.toBytes((Boolean) key);
        if (key instanceof BigDecimal) return Bytes.toBytes((BigDecimal) key);
        if (key instanceof ByteBuffer) return Bytes.toBytes((ByteBuffer) key);
        if (key instanceof Double) return Bytes.toBytes((Double) key);
        if (key instanceof Float) return Bytes.toBytes((Float) key);
        if (key instanceof Long) return Bytes.toBytes((Long) key);
        if (key instanceof Short) return Bytes.toBytes((Short) key);
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
