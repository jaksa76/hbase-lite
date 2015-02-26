package me.jaksa.hbase.lite;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Functions that can be serialized.
 *
 * @author Jaksa Vuckovic
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
