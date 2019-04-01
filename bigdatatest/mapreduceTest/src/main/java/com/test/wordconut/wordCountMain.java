package com.test.wordconut;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class wordCountMain extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), wordCountMain.class.getSimpleName());
        job.setJarByClass(wordCountMain.class);


        job.setInputFormatClass(TextInputFormat.class);

//        TextInputFormat.addInputPath(job,new Path("file:///D:\\testspace\\in"));

        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/wordcount"));

        //第二步：设置我们的mapper类
        job.setMapperClass(wordCountMapper.class);
        //设置我们map阶段完成之后的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //第三步，第四步，第五步，第六步，省略
        //第七步：设置我们的reduce类
        job.setReducerClass(wordCountReducer.class);
        //设置我们reduce阶段完成之后的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //第八步：设置输出类以及输出路径
        job.setOutputFormatClass(TextOutputFormat.class);


        TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/wordcount_out"));
//        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\testspace\\out"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;

    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Tool tool  =  new wordCountMain();
        int run = ToolRunner.run(configuration, tool, args);
        System.exit(run);


    }
}
