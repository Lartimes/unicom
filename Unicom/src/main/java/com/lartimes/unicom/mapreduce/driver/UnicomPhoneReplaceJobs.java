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
 * @author Lartimes
 * @version 1.0
 * @description:
 */
@Service
public class UnicomPhoneReplaceJobs {
    private final UnicomRawDriver unicomRawDriver;
    private final UnicomReplaceDriver unicomReplaceDriver;

    public UnicomPhoneReplaceJobs(UnicomReplaceDriver unicomReplaceDriver, UnicomRawDriver unicomRawDriver, UnicomReplaceDriver unicomReplaceDriver1) {
        this.unicomRawDriver = unicomRawDriver;
        this.unicomReplaceDriver = unicomReplaceDriver1;
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//        unicomPhoneReplaceJobs.cleanJobs("mysql-export", "replace_data", "replace_latest_data");
    }

    public void cleanJobs(String inputPath, String outputPath, String outputPath2) {
        inputPath = "mysql-export";
        outputPath = "replace_data";
        outputPath2 = "replace_latest_data";
        try {
            Configuration conf = new Configuration();
            conf.set("mapreduce.framework.name", "local");
            unicomRawDriver.setConf(conf);
            unicomReplaceDriver.setConf(conf);
            Job job = unicomReplaceDriver.job();
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


            JobControl jobCtrl = new JobControl("myctrl2");
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