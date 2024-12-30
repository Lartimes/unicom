package com.lartimes.unicom.mapreduce.driver;

import com.lartimes.unicom.mapreduce.groups.TextPartitionerComparator;
import com.lartimes.unicom.mapreduce.mr.UnicomMapper;
import com.lartimes.unicom.mapreduce.mr.UnicomReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Lartimes
 * @version 1.0
 * @description: imsi key ,value ---> imsi , 3文件数量的reduce 数量
 * imsi  ---> 进行数据清洗
 * 日期 --> 输出，
 * @since 2024/3/5 0:06
 */
public class UnicomDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");

        int run = ToolRunner.run(conf, new UnicomDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());
//        设置程序主类
        job.setJarByClass(this.getClass());

        //设置Map /Reduce 类
        job.setGroupingComparatorClass(TextPartitionerComparator.class);
        job.setMapperClass(UnicomMapper.class);
        job.setReducerClass(UnicomReducer.class);

//       设置map阶段输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
//        job.setNumReduceTasks(5);
//        job.setPartitionerClass(UnicomGroupByIMSI.class);
//        设置reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

//        job.submit();
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }
}
