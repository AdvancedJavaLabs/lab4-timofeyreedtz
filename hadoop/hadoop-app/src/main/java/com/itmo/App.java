package com.itmo;

import com.itmo.mapper.SalesMapper;
import com.itmo.model.SalesData;
import com.itmo.reducer.SalesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class App {

    private static final String JOB_NAME = "Sales Analysis";

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: hadoop jar <jar-file> <input-path> <output-path> <datablock-size-kb>");
            System.exit(-1);
        }

        String inputDir = args[0];
        String outputDir = args[1];
        int datablockSizeKb = Integer.parseInt(args[2]) * 1024; // Размер блока в KB

        List<Integer> reducerCounts = Arrays.asList(1, 2, 4, 8, 16);

        Configuration conf = createHadoopConfiguration(datablockSizeKb);

        for (int reducerCount : reducerCounts) {
            runJobWithReducer(inputDir, outputDir, reducerCount, conf);
        }
    }

    private static Configuration createHadoopConfiguration(int datablockSizeKb) {
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.memory.mb", "1024"); // 1GB для Map задачи
        conf.set("mapreduce.map.java.opts", "-Xmx800m"); // Ограничение 800MB на Map heap
        conf.set("mapreduce.reduce.memory.mb", "1024"); // 1GB для Reduce задачи
        conf.set("mapreduce.reduce.java.opts", "-Xmx800m"); // Ограничение 800MB на Reduce heap

        conf.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString(datablockSizeKb * 1024));
        conf.set("mapreduce.input.fileinputformat.split.minsize", Integer.toString(datablockSizeKb * 1024 / 2));

        return conf;
    }

    private static void runJobWithReducer(String inputDir, String outputDir, int reducerCount, Configuration conf) throws Exception {
        System.out.println("Running with " + reducerCount + " reducers...");

        long startTime = System.currentTimeMillis();
        Job job = createJob(inputDir, outputDir, reducerCount, conf);

        boolean success = job.waitForCompletion(true);

        if (success) {
            long elapsedTimeInSeconds = (System.currentTimeMillis() - startTime) / 1000;
            String result = "Reducers: " + reducerCount + " | Time: " + elapsedTimeInSeconds + " seconds\n";
            String resultFileName = generateResultFileName(reducerCount);
            appendMetricsToFile(new Path(resultFileName), result, conf);
        } else {
            System.err.println("Job failed with " + reducerCount + " reducers.");
        }
    }

    private static String generateResultFileName(int reducerCount) {
        return "/user/root/result/job_metrics_" + reducerCount + "_" + System.currentTimeMillis() + ".txt";
    }

    private static Job createJob(String inputDir, String outputDir, int reducersCount, Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, JOB_NAME + " with " + reducersCount + " reducers");
        job.setNumReduceTasks(reducersCount);
        job.setJarByClass(App.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        return job;
    }

    private static void appendMetricsToFile(Path resultFilePath, String result, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(resultFilePath)) {
            fs.delete(resultFilePath, true);
        }

        try (FSDataOutputStream outputStream = fs.create(resultFilePath)) {
            outputStream.writeBytes(result);
        }
    }
}