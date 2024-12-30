package com.lartimes.unicom.mapreduce.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 22:19
 */
public class UnicomMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text inKey = new Text();
    private final Text outKey = new Text();


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        if (key.get() == 1L) {
            return;
        }
        String line = value.toString();
        String[] arr = line.split(",", 3);
        if (arr[1] == null || arr[1].isEmpty()) { //如果imsi 为null 则该行无需插入，
            return;
        }
        inKey.set(arr[1]);
        outKey.set(line);
        context.write(inKey, outKey);
    }
}
