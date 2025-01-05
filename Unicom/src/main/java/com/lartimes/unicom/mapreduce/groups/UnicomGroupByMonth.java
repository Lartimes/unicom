package com.lartimes.unicom.mapreduce.groups;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class UnicomGroupByMonth extends HashPartitioner<Text, Text> {
    @Override
    public int getPartition(Text k, Text v, int numPartitions) {
//        System.out.println(k.toString());
        return Integer.parseInt(k.toString().replaceAll("\"" , "")) % 201500;
    }
}

