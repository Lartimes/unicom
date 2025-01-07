package com.lartimes.unicom.mapreduce.driver;

import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/7 16:45
 */
@Service
public class UnicomGroupDriver extends Configured implements Tool {
    @SneakyThrows
    public int doJob(String[] args) {
        args = new String[]{"replace_latest_data", "predict_dataset"};
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        int run = ToolRunner.run(conf, this, args);
        return run;
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
        TextOutputFormat.setOutputPath(job, output);
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(output)) {
            boolean delete = fs.delete(output, true);
            System.out.println(delete);
        }
        long now = System.currentTimeMillis();
        boolean b = job.waitForCompletion(true);
        long then = System.currentTimeMillis();
        System.out.print("用时:");
        System.out.println((then - now) / 1000L);
        return b ? 0 : 1;
    }

    private Job job() {
        try {
            Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());
            job.setMapperClass(GroupMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(0);
            return job;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class GroupMapper
            extends Mapper<LongWritable, Text, Text, NullWritable> {
        private final NullWritable outValue = NullWritable.get();

        private static int score(int value, int index) {
            if (index == 5) {
                if (value <= 2) {
                    return 30;
                }
                return 60;
            } else {
                if (value <= 2) {
                    return 10;
                } else if (value <= 6) {
                    return 45;
                } else {
                    return 45;
                }
            }
        }

        //    比例
//    max:    11334 参考量太大 后面说
//  max: 9286
//        流量 2      11   2， 6 ， 2
//        2 , 5 ,5
//        1.0
//0.0
//        语音  266
//        ARPU 2 7   1 , 1 2, 3 ,4 ,5 ,6
//         sms 16
//            赋权重值
//            月份,IMSI,网别,性别,年龄值段,ARPU值段,终端品牌,终端型号,流量使用量,语音通话时长,短信条数
//            201501,f92f1d156e7e61edc41ebc1c4fa51636,2G,男,5,1,HUAWEI,ETS3023,1,0,0,0,201501
//            只考虑 流量 和 arpu进行权重
//        201501,3G,7,15,201501
//            timenow ， net ， age，消费莫i行 ，change_time

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            String[] split = value.toString().split(",");
            if("201501".equals(split[0])){
                return;
            }
            sb.append(split[0]).append(",");
//            后续用好的权重算法代替
            int arpu = score(Integer.parseInt(split[5]), 5);
            int traffic = score(Integer.parseInt(split[8]), 8);
            sb.append(split[2]).append(",");
            sb.append(split[4]).append(",");
            sb.append((arpu + traffic) / 2).append(",");
            sb.append(split[split.length - 1]).append(",")
                    .append(split[split.length -2]);
            value.set(sb.toString());
            context.write(value, outValue);
        }

    }



}
