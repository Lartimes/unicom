package com.lartimes.unicom.mapreduce.mr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/4 14:42
 */
public class UnicomGroupToRawReducer extends Reducer<Text, Text, Text, NullWritable> {
    private  static  final  NullWritable outValue = NullWritable.get();
    private final  Text outKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            outKey.set(value.toString());
            context.write(outKey, outValue);
        }
    }
}
