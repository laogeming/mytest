package com.test.partitiontest;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper {

    @Override
    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {

        context.write(value , NullWritable.get());

    }
}
