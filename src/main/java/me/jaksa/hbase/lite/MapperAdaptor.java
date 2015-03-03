package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

/**
 * Groups all rows in the same reduce bucket
 *
 * @author Jaksa Vuckovic
 */
class MapperAdaptor<T, I extends Serializable> extends TableMapper<IntWritable, BytesWritable> {
    private final IntWritable ONE_KEY = new IntWritable(0);
    private Converter<T> converter;
    private List<SerializableFunction> mappers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Class<Converter<T>> converterClazz = (Class<Converter<T>>) context.getConfiguration().getClass("converter", Converter.class);
        try {
            converter = converterClazz.newInstance();
            mappers = TempStorage.getInstance().loadMapperFunctions(context);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IOException("the converter class must have a no-arg public constructor", e);
        }
    }

    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Object t = converter.convert(value);
        for (SerializableFunction mapper : mappers) {
            t = mapper.apply(t);
        }
        context.write(ONE_KEY, new BytesWritable(SerializableUtils.toBytes((Serializable) t)));
    }
}