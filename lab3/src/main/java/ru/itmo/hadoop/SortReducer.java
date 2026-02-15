package ru.itmo.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SortReducer
        extends Reducer<RevenueKey, LongWritable, Text, Text> {

    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    @Override
    protected void reduce(RevenueKey key,
                          Iterable<LongWritable> values,
                          Context ctx) throws IOException, InterruptedException {

        long sumQty = 0L;
        for (LongWritable v : values) {
            sumQty += v.get();
        }

        outKey.set(key.getCategory());
        outValue.set(String.format("%.2f\t%d", key.getRevenue(), sumQty));
        ctx.write(outKey, outValue);
    }
}