package com.lartimes.unicom.mapreduce.driver;

import com.lartimes.unicom.mapreduce.groups.TextPartitionerComparator;
import com.lartimes.unicom.mapreduce.mr.UnicomGroupCleanMapper;
import com.lartimes.unicom.mapreduce.mr.UnicomGroupCleanReducer;
import org.apache.commons.io.FileUtils;
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

import java.io.File;

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


    public Job job() {
        try {
            Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());
            //        设置程序主类
            job.setJarByClass(this.getClass());
            job.setMapperClass(UnicomGroupCleanMapper.class);
            job.setReducerClass(UnicomGroupCleanReducer.class);
//        job.setPartitionerClass(UnicomGroupByIMSI.class);
            job.setGroupingComparatorClass(TextPartitionerComparator.class);
            job.setNumReduceTasks(3);

//       设置map阶段输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
//        设置reduce输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            return job;
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = job();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        TextInputFormat.addInputPath(job, input);
        File dir = new File(output.toUri().getPath());
        if (dir.exists()) {
            File file = new File(dir + "_duplicate");
            try {
                FileUtils.moveDirectoryToDirectory(dir, file, true);
            } catch (Exception ignore) {

            } finally {
                boolean delete = dir.delete();
                System.out.println(delete ? "进行迁移之前的数据" : "迁移失败，未成功删除 exit ....");
            }
        }
        System.out.println("========");
        TextOutputFormat.setOutputPath(job, output);
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            System.out.println("存在");
            boolean delete = fs.delete(output, true);
            System.out.println(delete);
        }
        long now = System.currentTimeMillis();
        boolean b = job.waitForCompletion(true);
        long then = System.currentTimeMillis();
        System.out.print("用时:");
        System.out.println((then - now) /1000L  );
        return b ? 0 : 1;
    }
}