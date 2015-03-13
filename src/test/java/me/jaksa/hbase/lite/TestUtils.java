package me.jaksa.hbase.lite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * @author Jaksa Vuckovic
 */
public class TestUtils {
    public static final TestUtils.Dummy DUMMY_JOE = new TestUtils.Dummy("joe", "1");
    public static final Put DUMMY_PUT = new Put(new byte[1]);
    public static final Result DUMMY_RESULT = Result.create(new Cell[] {KeyValue.createKeyValueFromKey("joe".getBytes())});

    public static void createTestTable() throws IOException {
        HBaseAdmin hbase = new HBaseAdmin(HBaseLite.getConfiguration());
        if (!hbase.tableExists("test-table")) {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("test-table"));
            HColumnDescriptor family = new HColumnDescriptor("cf".getBytes());
            desc.addFamily(family);
            hbase.createTable(desc);
        }
    }

    public static void createEmployeeTable() throws IOException {
        HBaseAdmin hbase = new HBaseAdmin(HBaseLite.getConfiguration());
        if (!hbase.tableExists("Employees")) {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("Employees"));
            desc.addFamily(new HColumnDescriptor(Bytes.toBytes("cf")));
            desc.addFamily(new HColumnDescriptor(Bytes.toBytes("ext")));
            hbase.createTable(desc);
        }
    }

    static class Dummy implements Serializable {
        public final String name;
        public final String value;
        Dummy(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }

    public static class DummyConverter implements Converter<Dummy> {
        @Override
        public Dummy convert(Result result) {
            return new Dummy(
                    Bytes.toString(result.getRow()),
                    Bytes.toString(result.getValue(toBytes("cf"), toBytes("val")))
            );
        }

        @Override
        public Put toPut(Dummy dummy) {
            Put put = new Put(toBytes(dummy.name));
            put.add(toBytes("cf"), toBytes("val"), toBytes(dummy.value));
            return put;
        }
    }
}
