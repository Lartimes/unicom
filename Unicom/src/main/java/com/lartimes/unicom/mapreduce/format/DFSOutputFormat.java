package com.lartimes.unicom.mapreduce.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lartimes.unicom.mapreduce.bean.Unicom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.StringJoiner;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/4 18:54
 */
public class DFSOutputFormat extends FileOutputFormat<LongWritable, Text> {
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        Path file = getDefaultWorkFile(job, "");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(file, false);
        return new CustomRecordWriter(out);
    }

    private static class CustomRecordWriter extends RecordWriter<LongWritable, Text> {
        private final FSDataOutputStream out;
        private final ObjectMapper objectMapper = new ObjectMapper();
        private boolean getDate;
        private Integer date;
        {
            objectMapper.registerModule(new JavaTimeModule());
        }

        public CustomRecordWriter(FSDataOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(LongWritable key, Text value) throws IOException, InterruptedException {
            String str = value.toString().replaceAll("'","\"");
            Unicom unicom = objectMapper.readValue(str, Unicom.class);
            if (!getDate) {
                LocalDateTime timeNow = unicom.getTimeNow();
                int year = timeNow.getYear();
                int month = timeNow.getMonthValue();
                date = year * 10 + month;
                if(month < 10){
                    date = year * 100 + month;
                }
                getDate = true;
            }
//            月份,IMSI,网别,性别,年龄值段,ARPU值段,终端品牌,终端型号,流量使用量,语音通话时长,短信条数
            String string = new StringJoiner(",").add(String.valueOf(date)).add(unicom.getImsi())
                    .add(unicom.getImsi())
                    .add(unicom.getNet())
                    .add(unicom.getSex())
                    .add(String.valueOf(unicom.getAgeWeight()))
                    .add(unicom.getBrand())
                    .add(unicom.getModel())
                    .add(String.valueOf(unicom.getTrafficWeight()))
                    .add(String.valueOf(unicom.getCallSum()))
                    .add(String.valueOf(unicom.getSmsTotal())).toString();
            //TODO hbase <--> hdfs
            out.writeBytes(string + "\n");
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            out.close();
        }
    }
}
