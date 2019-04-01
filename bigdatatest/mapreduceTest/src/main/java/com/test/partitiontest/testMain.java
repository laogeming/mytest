package com.test.partitiontest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class testMain extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(super.getConf(), "mypartitiontest");

        job.setJarByClass(testMain.class);

        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job, new Path(strings[0]));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setPartitionerClass(Mypartition.class);

        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(2);


        job.setOutputFormatClass(TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path(strings[1]));

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int run = ToolRunner.run(new Configuration(), new testMain(), args);
        System.exit(run);

    }
}
