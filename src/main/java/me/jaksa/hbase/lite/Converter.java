package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/**
 * A Converter is supposed to convert between objects use by HBase and domain objects. Implement this
 * interface to store your objects into HBase.
 *
 * @param <T>
 */
public interface Converter<T> {
    /**
     * Extracts the columns from the result and creates the domain object.
     *
     * @param result the object from HBase
     * @return the domain object
     */
    T convert(Result result);

    /**
     * Creates a Put object usable by HBase from the domain object. You should insert all the column values in
     * the put object.
     *
     * @param t the domain object
     * @return
     */
    Put toPut(T t);
}
