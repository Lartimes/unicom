package com.lartimes.unicom.mapreduce.mr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.lartimes.unicom.mapreduce.bean.Unicom;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.StringJoiner;

/**
 * @author Lartimes
 * @version 1.0
 * @description:
 * @since 2024/3/5 22:46
 */
public class MysqlReaderMapperV2 extends Mapper<LongWritable, Unicom, NullWritable, Text> {
    private final NullWritable outKey = NullWritable.get();
    private final Text outValue = new Text();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private boolean getDate;
    private Integer date;
    {
        objectMapper.registerModule(new JavaTimeModule());
    }
    @Override
    protected void map(LongWritable key, Unicom value, Mapper<LongWritable, Unicom, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        Unicom unicom = objectMapper.readValue(value.toString().replaceAll("'","\""), Unicom.class);
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
        String string = new StringJoiner(",").add(String.valueOf(date)).add(unicom.getImsi())
                .add(unicom.getNet())
                .add(unicom.getSex())
                .add(String.valueOf(unicom.getAgeWeight()))
                .add(String.valueOf(unicom.getArpu()))
                .add(unicom.getBrand())
                .add(unicom.getModel())
                .add(String.valueOf(unicom.getTrafficWeight()))
                .add(String.valueOf(unicom.getCallSum()))
                .add(String.valueOf(unicom.getSmsTotal())).toString();
        outValue.set(string);
        context.write(outKey, outValue);
    }
}
