package com.lartimes.unicom.mapreduce.snappy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SnappyCodeWriteDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), SnappyCodeWriteDriver.class.getSimpleName());
        job.setJarByClass(SnappyCodeWriteDriver.class);
        job.setMapperClass(GzipMapper.class);
        job.setReducerClass(GzipReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileInputFormat.addInputPath(job , input);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, output);

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        //配置输出结果压缩为Gzip格式
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec",
                "org.apache.hadoop.io.compress.SnappyCodec");

        int status = ToolRunner.run(conf, new SnappyCodeWriteDriver(), args);
        System.exit(status);
    }
    private static final NullWritable outKey = NullWritable.get();
    public static class GzipMapper extends Mapper<LongWritable ,Text , NullWritable , Text > {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            context.write(outKey , value);
        }
    }

    public static class GzipReducer extends Reducer<NullWritable ,Text , NullWritable , Text> {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(outKey , value);
            }
        }
    }
}
