package com.lartimes.unicom.utils;

import com.lartimes.unicom.model.po.Unicom;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/6 22:54
 */
@Repository
public class HBaseUtils {
    private static final byte[] TABLE_NAME = "unicom".getBytes();
    private static final String FAMILY_BASEINFO = "BASEINFO";
    private static final String FAMILY_CONSUME = "CONSUME";
    @Autowired
    private Connection connection;

    //    进行插入 根据行插入
//    根据行键查询
    @SneakyThrows
    public void batchInsert() {
        String dir = System.getProperty("user.dir").replaceAll("\\\\", "/")
                + "/mysql-export/";
        Admin admin = connection.getAdmin();
        // 检查表是否存在，如果不存在则创建
        if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(FAMILY_BASEINFO));
            tableDescriptor.addFamily(new HColumnDescriptor(FAMILY_CONSUME));
            admin.createTable(tableDescriptor);
            System.out.println("Table created: " + TABLE_NAME);
        }
        List<Put> puts = new ArrayList<>();
        try (BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(TABLE_NAME))) {
            for (File file : Objects.requireNonNull(new File(dir).listFiles())) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    Unicom unicom = new Unicom(line);
                    puts.add(convertToPut(unicom));
                    if (puts.size() % 3000 == 0) {
                        mutator.mutate(puts);
                        mutator.flush();
                        System.out.println("插入");
                        puts.clear();
                    }
                }
                mutator.mutate(puts);
                System.out.println("插入");
                mutator.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    private Put convertToPut(Unicom unicom) {
        Put put = new Put(Bytes.toBytes(unicom.getImsi()));
//        201501,decf8f2fffff42b4746b6bd02d10dd23,2G,男,6,1,None,None,1,0,0
//        月份,IMSI,网别,性别,年龄值段,
//        ARPU值段,终端品牌,终端型号,流量使用量,语音通话时长,短信条数
        put.addColumn(Bytes.toBytes(FAMILY_BASEINFO), Bytes.toBytes("sex"), Bytes.toBytes(unicom.getSex()));
        put.addColumn(Bytes.toBytes(FAMILY_BASEINFO), Bytes.toBytes("net"), Bytes.toBytes(unicom.getNet()));
        put.addColumn(Bytes.toBytes(FAMILY_BASEINFO), Bytes.toBytes("age_weight"), Bytes.toBytes(unicom.getAgeWeight()));
        put.addColumn(Bytes.toBytes(FAMILY_BASEINFO), Bytes.toBytes("time_now"), Bytes.toBytes(String.valueOf(unicom.getTimeNow())));
        put.addColumn(Bytes.toBytes(FAMILY_CONSUME), Bytes.toBytes("arpu"), Bytes.toBytes(unicom.getArpu()));
        put.addColumn(Bytes.toBytes(FAMILY_CONSUME), Bytes.toBytes("brand"), Bytes.toBytes(unicom.getSex()));
        put.addColumn(Bytes.toBytes(FAMILY_CONSUME), Bytes.toBytes("model"), Bytes.toBytes(unicom.getModel()));
        put.addColumn(Bytes.toBytes(FAMILY_CONSUME), Bytes.toBytes("traffic_weight"), Bytes.toBytes(unicom.getTrafficWeight()));
        put.addColumn(Bytes.toBytes(FAMILY_CONSUME), Bytes.toBytes("call_sum"), Bytes.toBytes(unicom.getCallSum()));
        put.addColumn(Bytes.toBytes(FAMILY_CONSUME), Bytes.toBytes("sms_total"), Bytes.toBytes(unicom.getSmsTotal()));
        return put;
    }


}
