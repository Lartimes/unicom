package com.lartimes.unicom.mapreduce.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/4 15:25
 */
public class UnicomCleanJobs {
    public void cleanJobs(){
        Configuration conf = new Configuration();
        //TODO ControlledJob
        Job job = Job.getInstance(conf, GoodsJoinDriver.class.getSimpleName());
        job.setJarByClass(GoodsJoinDriver.class);
        job.setMapperClass(GoodsJoinMapper.class);
        job.setReducerClass(GoodsJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("D:\\Dev\\BigDataDev\\learning\\HadoopDev\\join-out"));
        FileInputFormat.addInputPath(job, new Path("D:\\Dev\\BigDataDev\\learning\\HadoopDev\\goodsInput\\*"));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //将普通作业包装成受控作业
        ControlledJob ctrljob1 = new ControlledJob(conf);
        ctrljob1.setJob(job);


        Job job2 = Job.getInstance(conf, GoodsSortDriver.class.getSimpleName());
        job2.setJarByClass(GoodsSortDriver.class);
        job2.setMapperClass(GoodsSortMapper.class);
        job2.setReducerClass(GoodsSortReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileOutputFormat.setOutputPath(job2, new Path("D:\\Dev\\BigDataDev\\learning\\HadoopDev\\join-sort-out"));
        FileInputFormat.addInputPath(job2, new Path("D:\\Dev\\BigDataDev\\learning\\HadoopDev\\join-out\\*"));

        ControlledJob ctrljob2 = new ControlledJob(conf);
        ctrljob2.setJob(job2);


        //设置依赖job的依赖关系
        ctrljob2.addDependingJob(ctrljob1);
        // 主控制容器，控制上面的总的两个子作业
        JobControl jobCtrl = new JobControl("myctrl");

        // 添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);
// 在线程启动，记住一定要有这个
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {
            if (jobCtrl.allFinished()) {// 如果作业成功完成，就打印成功作业的信息
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
    }
}
