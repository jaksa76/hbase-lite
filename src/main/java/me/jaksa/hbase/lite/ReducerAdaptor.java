package me.jaksa.hbase.lite;

import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.Serializable;

/**
 * Wraps the user supplied reducer function and extends the HBase TableReducer.
 *
 * @param <I> the intermediate type coming from the map step
 * @param <R> the type of the result
 *
 * @author Jaksa Vuckovic
 */
public class ReducerAdaptor<I, R extends Serializable> extends TableReducer<IntWritable, BytesWritable, Text> {
    private SerializableFunction<Iterable<I>, R> reducerFunction;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        reducerFunction = TempStorage.getInstance().loadReducerFunction(context);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        Iterable<I> domainObjects = Iterables.transform(values, result -> {
            try {
                return (I) SerializableUtils.fromBytes(result.getBytes());
            } catch (Exception e) {
                return null;
            }
        });

        R result = reducerFunction.apply(domainObjects);

        TempStorage.getInstance().storeResult(context, result);
    }
}
