package ru.itmo.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, CategoryStats, Text, CategoryStats> {
    private static final Logger LOG = LoggerFactory.getLogger(Reducer.class);

    private final static CategoryStats result = new CategoryStats();
    private String metric;

    @Override
    protected void setup(Context context) {
        metric = context.getConfiguration().get("metric", "sum");
        LOG.info("Used action: {}", metric);
    }

    @Override
    protected void reduce(Text key, Iterable<CategoryStats> values, Context context) 
            throws IOException, InterruptedException {
        
        double sumRevenue = 0;
        int sumQuantity = 0;
        
        for (CategoryStats stats : values) {
            sumRevenue += stats.getRevenue();
            sumQuantity += stats.getQuantity();
        }
        
        result.set(sumRevenue, sumQuantity);
        context.write(key, result);
    }
}