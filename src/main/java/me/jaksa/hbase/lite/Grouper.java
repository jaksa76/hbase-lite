package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Groups all rows in the same reduce bucket
 */
public class Grouper extends TableMapper<IntWritable, Result> {
    private final IntWritable ONE_KEY = new IntWritable(0);
    protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException, InterruptedException {
        context.write(ONE_KEY, value);
    }
}