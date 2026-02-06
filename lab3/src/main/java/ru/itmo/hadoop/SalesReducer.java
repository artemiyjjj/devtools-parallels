package ru.itmo.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.itmo.hadoop.Metrics.Metric;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(Reducer.class);

    private final static DoubleWritable totalRevenue = new DoubleWritable();
    private String metric;

    @Override
    protected void setup(Context context) {
        metric = context.getConfiguration().get("metric", "sum");
        LOG.info("Used action: {}", metric);
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        double sumRevenue = 0;
        double result = 0;
        int count = 0;
        
        for (DoubleWritable value : values) {
            sumRevenue += value.get();
            count++;
        }

        switch (Metric.valueOf(metric.toUpperCase())) {
            case AVERAGE:
                result = sumRevenue / count;
                break;
            case COUNT:
                result = count;
                break;
            default:
                LOG.warn("Unknown metric {} is applied. Using sum", metric);
            case SUM:
                result = sumRevenue;
        }
        
        totalRevenue.set(result);
        context.write(key, totalRevenue);
    }
}