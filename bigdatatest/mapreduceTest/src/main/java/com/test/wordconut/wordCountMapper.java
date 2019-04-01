package com.test.wordconut;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class wordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String Line = value.toString();

       String[] split = Line.split(",");

        for (String word : split) {

            context.write(new Text(word),new LongWritable(1));
            
        }
    }
}
