package com.lartimes.unicom.mapreduce.driver;

import com.lartimes.unicom.mapreduce.bean.Unicom;
import com.lartimes.unicom.mapreduce.mr.MysqlReaderMapperV2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Objects;

/**
 * @author Lartimes
 * @version 1.0
 * @description: 先生成本地，再上传
 * There are 1 datanode(s) running and 1 node(s) are excluded in this operation.
 * 此ERROR 还不知如何解决
 * @since 2024/3/5 22:57
 */
@Service
public class MysqlDBReaderDriverV2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        for (int i = 201501; i <= 201512; i++) {
            int status = ToolRunner.run(conf, new MysqlDBReaderDriverV2(), new String[]{String.valueOf(i)});
            System.out.println(status == 0 ? "SUCCESS" : "FAIL");
        }
        final String uri = "hdfs://" + "localhost" + ":" + 8020;
        conf.set("fs.defaultFS", uri);
        FileSystem fileSystem = FileSystem.get(conf);
        String curDir = System.getProperty("user.dir").replaceAll("\\\\" , "/");
        System.out.println(curDir);
        fileSystem.copyFromLocalFile(false , false ,new Path("file:///" +curDir +"/mysql-export/"), new Path(uri + "/mysql-export"));
    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = getConf();
        DBConfiguration.configureDB(conf, "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://localhost:3306/unicom?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=UTF-8&useLegacyDatetimeCode=false&allowPublicKeyRetrieval=true",
                "root",
                "307314");
        Job job = Job.getInstance(conf, MysqlDBReaderDriverV2.class.getSimpleName());
        job.setJarByClass(MysqlDBReaderDriverV2.class);
        job.setMapperClass(MysqlReaderMapperV2.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path path = new Path(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("")).toURI());
        path = path.getParent().getParent().getParent().getParent();
        String dest = "file:///" + path.toString().substring("file:/".length()) + "/export_data/part-r-00000";
        FileSystem fileSystem = FileSystem.get(conf);
        Path destPath = new Path(dest);
        if (fileSystem.exists(destPath)) {
            fileSystem.delete(destPath, true);
        }
        TextOutputFormat.setOutputPath(job, destPath);
        DBInputFormat.setInput(
                job,
                Unicom.class,
                "SELECT  imsi , time_now ,net ,sex ,age_weight , arpu , brand , model , traffic_weight , call_sum , sms_total from unicom_201501",
                "SELECT count(id) from unicom_" + args[0]
        );
        job.waitForCompletion(true);
        Path localPath = new Path(dest);
        String tmp = "mysql-export/part-r-000";
        int month = Integer.parseInt(args[0]) % 100;
        if (month < 10) {
            tmp += "0";
        }
        Path remotePath = new Path(tmp + month);
        try {
            fileSystem.copyFromLocalFile(false, true, localPath, remotePath);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
        return 0;
    }
}