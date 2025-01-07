package com.lartimes.unicom.mapreduce.driver;

import com.lartimes.unicom.mapreduce.groups.UnicomGroupByMonth;
import com.lartimes.unicom.mapreduce.mr.UnicomGroupToRawMapper;
import com.lartimes.unicom.mapreduce.mr.UnicomGroupToRawReducer;
import lombok.SneakyThrows;
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
import org.springframework.stereotype.Service;

import java.io.File;

@Service
public class UnicomRawDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        int run = ToolRunner.run(conf, new UnicomRawDriver(), new String[]{"replace_data" , "replace_latest_data"});
        System.exit(run);
    }

    @SneakyThrows
    public int doJob(String[] args) {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        int run = ToolRunner.run(conf, this, args);
        return run;
    }

    public Job job() {
        try {
            Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());
            job.setMapperClass(UnicomGroupToRawMapper.class);
            job.setReducerClass(UnicomGroupToRawReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setPartitionerClass(UnicomGroupByMonth.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(14);
            return job;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = job();
//
        Path input = new Path("data-out");
        Path output = new Path("data-out2");
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
        TextOutputFormat.setOutputPath(job, output);
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        long now = System.currentTimeMillis();
        boolean b = job.waitForCompletion(true);
        long then = System.currentTimeMillis();
        System.out.print("用时:");
        System.out.println((then - now) / 1000L);
        return b ? 0 : 1;
    }
}