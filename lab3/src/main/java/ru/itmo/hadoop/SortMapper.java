package ru.itmo.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper
        extends Mapper<Text, Text, RevenueKey, LongWritable> {

    private final RevenueKey outKey   = new RevenueKey();
    private final LongWritable outVal = new LongWritable();

    @Override
    protected void map(Text key, Text value, Context ctx)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split("\\t");
        if (parts.length != 2) {
            return;
        }

        double revenue = Double.parseDouble(parts[0]);
        long   quantity     = Long.parseLong(parts[1]);

        outKey.set(revenue, key.toString());
        outVal.set(quantity);
        ctx.write(outKey, outVal);
    }
}