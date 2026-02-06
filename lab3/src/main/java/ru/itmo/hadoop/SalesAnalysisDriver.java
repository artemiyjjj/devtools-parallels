package ru.itmo.hadoop;

import java.sql.Driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesAnalysisDriver {
    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);
    public static void main(String[] args) throws Exception {
        String inputPath, outputPath, metric;
        Configuration conf = new Configuration();
        Job job;
        if (args.length < 3) {
            System.err.println("Usage: hadoop jar <path/to/jar> <input/path> <output/path> <metric>");
            System.exit(-1);
        }
        inputPath = args[0]; LOG.info("Input: {}", inputPath);
        outputPath = args[1]; LOG.info("Output: {}", outputPath);
        metric = args[2]; LOG.info("Metric: {}", metric);

        if (!Metrics.isValid(metric)) {
            LOG.error("Invalid metric: " + metric);
            System.exit(-1);
        }

        conf.set("metric", metric);
        job = Job.getInstance(conf, "sales analysis");
        
        job.setJarByClass(SalesAnalysisDriver.class);
        job.setMapperClass(SalesMapper.class);
        // job.setCombinerClass(SalesReducer.class);
        job.setReducerClass(SalesReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}