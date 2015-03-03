package me.jaksa.hbase.lite;

/**
 * This is just a SerializableFunction. It exists just to be able to distinguish partitioners from mappers.
 *
 * @author Jaksa Vuckovic
 */
public interface PartitionFunction<T, R> extends SerializableFunction<T, R> {
}
