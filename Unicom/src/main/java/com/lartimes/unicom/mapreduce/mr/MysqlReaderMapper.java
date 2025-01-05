package com.lartimes.unicom.mapreduce.mr;

import com.lartimes.unicom.mapreduce.bean.Unicom;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Lartimes
 * @version 1.0
 * @description:
 * @since 2024/3/5 22:46
 */
public class MysqlReaderMapper extends Mapper<LongWritable, Unicom, LongWritable, Text> {
    private final LongWritable outKey = new LongWritable();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Unicom value, Mapper<LongWritable, Unicom, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        outKey.set(key.get());
        outValue.set(value.toString());
        context.write(outKey, outValue);
    }
}
