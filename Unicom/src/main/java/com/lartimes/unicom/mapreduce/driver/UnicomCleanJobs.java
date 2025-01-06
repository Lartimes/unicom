package com.lartimes.unicom.mapreduce.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/4 15:25
 */
@Service
public class UnicomCleanJobs {
    private final UnicomDriver unicomDriver;
    private final UnicomRawDriver unicomRawDriver;

    public UnicomCleanJobs(UnicomDriver unicomDriver, UnicomRawDriver unicomRawDriver) {
        this.unicomDriver = unicomDriver;
        this.unicomRawDriver = unicomRawDriver;
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//        UnicomCleanJobs unicomCleanJobs = new UnicomCleanJobs();
//        unicomCleanJobs.cleanJobs("data", "data-out", "clean_data");
    }

    public void cleanJobs(String inputPath, String outputPath, String outputPath2) {
        inputPath = "data";
        outputPath = "data-out";
        outputPath2 = "clean_data";
        try {
            Configuration conf = new Configuration();
            conf.set("mapreduce.framework.name", "local");
            unicomRawDriver.setConf(conf);
            unicomDriver.setConf(conf);
            Job job = unicomDriver.job();
            FileSystem fs = FileSystem.get(conf);

            Path in = new Path(inputPath);
            Path out1 = new Path(outputPath);
            Path out2 = new Path(outputPath2);
            if (fs.exists(out1)) {
                fs.delete(out1, true);
            }
            if (fs.exists(out2)) {
                fs.delete(out2, true);
            }
            FileOutputFormat.setOutputPath(job, out1);
            FileInputFormat.addInputPath(job, in);
            ControlledJob ctrljob1 = new ControlledJob(conf);
            ctrljob1.setJob(job);

            Job job2 = unicomRawDriver.job();
            FileOutputFormat.setOutputPath(job2, out2);
            FileInputFormat.addInputPath(job2, out1);
            ControlledJob ctrljob2 = new ControlledJob(conf);
            ctrljob2.setJob(job2);
            ctrljob2.addDependingJob(ctrljob1);


            JobControl jobCtrl = new JobControl("myctrl");
            jobCtrl.addJob(ctrljob1);
            jobCtrl.addJob(ctrljob2);
            Thread t = new Thread(jobCtrl);
            t.start();
            long now = System.currentTimeMillis();
            while (true) {
                if (jobCtrl.allFinished()) {
                    System.out.println(jobCtrl.getSuccessfulJobList());
                    jobCtrl.stop();
                    break;
                }
            }
            long then = System.currentTimeMillis();
            System.out.print("用时:");
            System.out.println((then - now) / 1000L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
