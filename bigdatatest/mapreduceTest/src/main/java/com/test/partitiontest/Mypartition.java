package com.test.partitiontest;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Mypartition extends Partitioner<Text,NullWritable> {


    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {

        String result = text.toString().split("\t")[5];

        if (Integer.parseInt(result) > 15){
            return 1;
        }else{
            return 0;
        }

    }
}
