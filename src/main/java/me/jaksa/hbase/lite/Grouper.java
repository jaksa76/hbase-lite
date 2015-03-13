package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * Groups all rows in the same reduce bucket
 *
 * @author Jaksa Vuckovic
 */
class Grouper<T extends Serializable> extends TableMapper<BytesWritable, BytesWritable> {
    private final BytesWritable ONE_KEY = new BytesWritable(new byte[] {42});
    private Converter<T> converter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        converter = TempStorage.getInstance().retrieveConverter(context);
    }

    protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException, InterruptedException {
        T t = converter.convert(value);
        context.write(ONE_KEY, new BytesWritable(SerializableUtils.toBytes(t)));
    }
}