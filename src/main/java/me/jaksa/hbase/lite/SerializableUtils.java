package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.math.BigDecimal;

/**
 * @author Jaksa Vuckovic
 */
class SerializableUtils {
    public static byte[] toBytes(Serializable o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(o);
        return baos.toByteArray();
    }

    public static Object fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
        return in.readObject();
    }

    public static Object fromBytes(byte[] byteValue, Class<?> type) throws IOException, ClassNotFoundException {
        if (type.equals(Integer.class)) {
            return Bytes.toInt(byteValue);
        } else if (type.equals(String.class)) {
            return Bytes.toString(byteValue);
        } else if (type.equals(Long.class)) {
            return Bytes.toLong(byteValue);
        } else if (type.equals(Boolean.class)) {
            return Bytes.toBoolean(byteValue);
        } else if (type.equals(Double.class)) {
            return Bytes.toDouble(byteValue);
        } else if (type.equals(BigDecimal.class)) {
            return Bytes.toBigDecimal(byteValue);
        } else if (type.equals(Float.class)) {
            return Bytes.toFloat(byteValue);
        } else if (type.equals(Short.class)) {
            return Bytes.toShort(byteValue);
        } else {
            return fromBytes(byteValue);
        }
    }

    public static byte[] toBytes(Object byteValue, Class<?> type) throws IOException {
        if (type.equals(Integer.class)) {
            return Bytes.toBytes((Integer) byteValue);
        } else if (type.equals(String.class)) {
            return Bytes.toBytes((String) byteValue);
        } else if (type.equals(Long.class)) {
            return Bytes.toBytes((Long) byteValue);
        } else if (type.equals(Boolean.class)) {
            return Bytes.toBytes((Boolean) byteValue);
        } else if (type.equals(Double.class)) {
            return Bytes.toBytes((Double) byteValue);
        } else if (type.equals(BigDecimal.class)) {
            return Bytes.toBytes((BigDecimal) byteValue);
        } else if (type.equals(Float.class)) {
            return Bytes.toBytes((Float) byteValue);
        } else if (type.equals(Short.class)) {
            return Bytes.toBytes((Short) byteValue);
        } else if (Serializable.class.isAssignableFrom(type)) {
            return toBytes((Serializable) byteValue);
        }
        throw new IllegalArgumentException("Could not deserialize type " + type.getName());
    }
}
