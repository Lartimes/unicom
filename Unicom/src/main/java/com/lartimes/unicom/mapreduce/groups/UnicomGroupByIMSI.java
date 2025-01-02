package com.lartimes.unicom.mapreduce.groups;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class UnicomGroupByIMSI extends HashPartitioner<Text, Text> {
    @Override
    public int getPartition(Text k, Text v, int numPartitions) {
        return (k.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}

