package me.jaksa.hbase.lite;

import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;

import static me.jaksa.hbase.lite.SerializableUtils.fromBytes;
import static me.jaksa.hbase.lite.SerializableUtils.toBytes;
import static org.junit.Assert.*;

public class SerializableUtilsTest {
    @Test
    public void testTypeSpecificSerialization() throws Exception {
        testToBytesAndBack("one", String.class);
        testToBytesAndBack(42L, Long.class);
        testToBytesAndBack(new Long(42L), Long.class);
        testToBytesAndBack(42.42, Double.class);
        testToBytesAndBack(new Double(42.42), Double.class);
        testToBytesAndBack(42, Integer.class);
        testToBytesAndBack(new Integer(42), Integer.class);
        testToBytesAndBack(42.42f, Float.class);
        testToBytesAndBack((short) 42, Short.class);
        testToBytesAndBack(new Short((short) 42), Short.class);
        testToBytesAndBack(false, Boolean.class);
        testToBytesAndBack(new BigDecimal("999999999999999999999999999999999"), BigDecimal.class);
    }

    private void testToBytesAndBack(Object value, Class type) throws IOException, ClassNotFoundException {
        assertEquals(value, toBytesAndBack(type, value));
    }

    private Object toBytesAndBack(Class type, Object value) throws IOException, ClassNotFoundException {
        return fromBytes(toBytes(value, type), type);
    }
}