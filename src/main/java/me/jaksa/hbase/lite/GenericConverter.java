package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * A generic converter that uses JPA annotations to map the given class to HBase columns.
 *
 * @author Jaksa Vuckovic
 */
class GenericConverter<T> implements Converter<T> {
    private Class<T> clazz;
    private Field keyField;
    private Map<Field, HColumn> columns;

    public GenericConverter() {}

    public GenericConverter(Class<T> clazz) {
        setElementClass(clazz);
    }

    // this setter is used when we instantiate the converter on the workers
    void setElementClass(Class<T> clazz) {
        this.clazz = clazz;
        keyField = JPAUtils.getKeyField(clazz);
        keyField.setAccessible(true);
        this.columns = JPAUtils.getColumns(clazz);
        for (Field f : columns.keySet()) {
            f.setAccessible(true);
        }
    }

    public Class<T> getElementClass() {
        return clazz;
    }

    @Override
    public T convert(Result result) {
        T t;
        try {
            t = clazz.newInstance();

            keyField.set(t, SerializableUtils.fromBytes(result.getRow(), keyField.getType()));

            for (Field f : columns.keySet()) {
                HColumn column = columns.get(f);
                byte[] byteValue = result.getValue(column.family, column.name);
                Object value = SerializableUtils.fromBytes(byteValue, f.getType());
                f.set(t, value);
            }
            return t;
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Put toPut(T t) {
        try {
            Object keyValue = keyField.get(t);
            Put put = new Put(SerializableUtils.toBytes(keyValue, keyField.getType()));

            for (Field f : columns.keySet()) {
                HColumn column = columns.get(f);
                Object value = f.get(t);
                byte[] bytesValue = SerializableUtils.toBytes(value, f.getType());
                put.addColumn(column.family, column.name, bytesValue);
            }

            return put;
        } catch (IllegalAccessException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
