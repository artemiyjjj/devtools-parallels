package ru.itmo.hadoop;

import java.sql.Driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SalesSortDriver extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

    @Override
    public int run(String[] args) throws Exception {
        String inputPath, tmpPath, outputPath;
        int linesPerMap = 1000;
        Configuration conf;
        Job job;
        if (args.length < 3) {
            System.err.println("Usage: hadoop jar <path/to/jar> <input/path> <output/path> --linesPerMap <N>");
            System.exit(-1);
        }
        inputPath = args[0]; LOG.info("Input: {}", inputPath);
        outputPath = args[1]; LOG.info("Output: {}", outputPath);
        tmpPath = args[2]; LOG.info("Tmp : {}", tmpPath);

        for (int i = 3; i < args.length; i++) {
            if ("--linesPerMap".equals(args[i]) && i + 1 < args.length) {
                linesPerMap = Integer.parseInt(args[++i]);
            }
        }
        LOG.info("Lines per map : {}", linesPerMap);

        conf = getConf();
        conf.setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
        conf.setInt("mapreduce.job.split.metainfo.maxsize", 50 * 1024 * 1024);
        job = Job.getInstance(conf, "sales agregation");
        job.setJarByClass(SalesSortDriver.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(inputPath));

        job.setMapperClass(SalesMapper.class);
        job.setCombinerClass(SalesReducer.class);
        job.setReducerClass(SalesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CategoryStats.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CategoryStats.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(tmpPath));
        
        if (!job.waitForCompletion(false)) {
            System.exit(1);
        }

        Job jobSort = Job.getInstance(new Configuration(),
                "sales sort");
        jobSort.setJarByClass(SalesSortDriver.class);

        jobSort.setMapperClass(SortMapper.class);
        jobSort.setReducerClass(SortReducer.class);
        jobSort.setNumReduceTasks(1);

        jobSort.setMapOutputKeyClass(RevenueKey.class);
        jobSort.setMapOutputValueClass(LongWritable.class);
        jobSort.setOutputKeyClass(Text.class);
        jobSort.setOutputValueClass(Text.class);

        jobSort.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(jobSort, new Path(tmpPath));
        TextOutputFormat.setOutputPath(jobSort, new Path(outputPath));

        return jobSort.waitForCompletion(false) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
				new SalesSortDriver(), args);
		System.exit(exitCode); 
    }
}