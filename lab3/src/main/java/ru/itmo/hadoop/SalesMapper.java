package ru.itmo.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final static Text category = new Text();
    private final static DoubleWritable revenue = new DoubleWritable();
    private static final Logger LOG = LoggerFactory.getLogger(SalesMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (key.get() == 0) return; // Skip csv header

        String[] fields = line.split(",");
        if (fields.length < 5) {
            LOG.warn("Invalid line: {}", line);
            return;
        } else {
            String categoryValue = fields[2];
            double price = Double.parseDouble(fields[3]);
            int quantity = Integer.parseInt(fields[4]);
            
            category.set(categoryValue);
            revenue.set(price * quantity);
            
            context.write(category, revenue);
        }
    }
}