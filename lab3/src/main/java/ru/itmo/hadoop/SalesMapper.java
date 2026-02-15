package ru.itmo.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, CategoryStats> {
    private final static Text category = new Text();
    private final static CategoryStats stats = new CategoryStats();
    private static final Logger LOG = LoggerFactory.getLogger(SalesMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("transaction_id")) {
            return; // Skip csv header
        }

        String[] fields = line.split(",");
        if (fields.length < 5) {
            LOG.warn("Invalid line: {}", line);
            return;
        }

        try {
            String categoryValue = fields[2];
            double price = Double.parseDouble(fields[3]);
            long quantity = Long.parseLong(fields[4]);
            category.set(categoryValue);
            stats.set(price * quantity, quantity);
            context.write(category, stats);
        } catch (IOException | InterruptedException | NumberFormatException e) {
            LOG.error("Failed parse line: {}", line, e);
        }
    }
}