package me.jaksa.hbase.lite;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Groups all rows in the same reduce bucket
 *
 * @author Jaksa Vuckovic
 */
class MapperAdaptor<T, I extends Serializable> extends TableMapper<BytesWritable, BytesWritable> {
    private Converter<T> converter;
    private List<SerializableFunction> mappers;
//    private List<SerializableFunction> partitioners;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Class<Converter<T>> converterClazz = (Class<Converter<T>>) context.getConfiguration().getClass("converter", Converter.class);
        try {
            converter = converterClazz.newInstance();
            mappers = TempStorage.getInstance().loadMapperFunctions(context);
//            partitioners = TempStorage.getInstance().loadPartitionerFunctions(context);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IOException("the converter class must have a no-arg public constructor", e);
        }
    }

    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Object t = converter.convert(value);
        ArrayList outKey = new ArrayList<>();
        for (SerializableFunction mapper : mappers) {
            if (mapper instanceof PartitionFunction) {
                outKey.add(mapper.apply(t));
            } else {
                t = mapper.apply(t);
            }
        }

        context.write(new BytesWritable(SerializableUtils.toBytes(outKey)),
                new BytesWritable(SerializableUtils.toBytes((Serializable) t)));
    }
}