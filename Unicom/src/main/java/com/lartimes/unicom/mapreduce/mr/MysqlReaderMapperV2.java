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
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author Lartimes
 * @version 1.0
 * @description:
 */
public class MysqlReaderMapperV2 extends Mapper<LongWritable, Unicom, NullWritable, Text> {
    private final NullWritable outKey = NullWritable.get();
    private final Text outValue = new Text();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final LongAdder[] arr = new LongAdder[12];

    {
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    protected void map(LongWritable key, Unicom value, Mapper<LongWritable, Unicom, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        Unicom unicom = objectMapper.readValue(value.toString().replaceAll("'", "\""), Unicom.class);
        LocalDateTime timeNow = unicom.getTimeNow();
        int monthValue = timeNow.getMonthValue();
        if (arr[monthValue - 1] == null) {
            arr[monthValue - 1] = new LongAdder();
            int year = timeNow.getYear();
            arr[monthValue - 1].add(year * 100L + monthValue);
            System.out.println(Arrays.toString(arr));
        }
        String string = new StringJoiner(",").add(String.valueOf(arr[monthValue - 1].intValue())).add(unicom.getImsi())
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
