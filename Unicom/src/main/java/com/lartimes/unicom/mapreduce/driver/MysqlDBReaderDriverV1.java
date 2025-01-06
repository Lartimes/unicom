package com.lartimes.unicom.mapreduce.driver;

import com.lartimes.unicom.mapreduce.bean.Unicom;
import com.lartimes.unicom.mapreduce.format.DFSOutputFormat;
import com.lartimes.unicom.mapreduce.mr.MysqlReaderMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Lartimes
 * @version 1.0
 * @description:
 * @since 2024/3/5 22:57
 */
public class MysqlDBReaderDriverV1 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration conf = new Configuration();
        final String uri = "hdfs://" + "localhost" + ":" + 8020;
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", uri);
        int status = ToolRunner.run(conf, new MysqlDBReaderDriverV1(), args);
        System.exit(status);

    }

    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = getConf();
        DBConfiguration.configureDB(conf, "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://localhost:3306/unicom?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=UTF-8&useLegacyDatetimeCode=false&allowPublicKeyRetrieval=true",
                "root",
                "307314");
        Job job = Job.getInstance(conf, MysqlDBReaderDriverV1.class.getSimpleName());
        job.setJarByClass(MysqlDBReaderDriverV1.class);
        job.setMapperClass(MysqlReaderMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DFSOutputFormat.class);
        DBInputFormat.setInput(
                job,
                Unicom.class,
                "SELECT  imsi , time_now ,net ,sex ,age_weight , arpu , brand , model , traffic_weight , call_sum , sms_total from unicom_201501",
                "SELECT count(id) from unicom_201501"
        );
        DFSOutputFormat.setOutputPath(job, new Path("mysql-out2"));


        return job.waitForCompletion(true) ? 0 : 1;
    }
}
