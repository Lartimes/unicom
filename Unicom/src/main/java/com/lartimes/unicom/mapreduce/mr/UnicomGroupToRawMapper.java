package com.lartimes.unicom.mapreduce.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/4 14:42
 */
public class UnicomGroupToRawMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final NullWritable nullWritable = NullWritable.get();
    private final Text outKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if(!split[0].contains("月份")){
            outKey.set(split[0]);
            context.write(outKey, value);
        }
    }
}
