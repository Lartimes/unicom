package com.lartimes.unicom.conf;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/6 22:40
 */
@Configuration
public class HBaseConf {

    @Value("${hbase.addr}")
    private String addr;

    @Bean
    public Connection getConnection() {
        return null;
//        org.apache.hadoop.conf.Configuration conf  = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "localhost");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
////        TODO 上传获取数据
////        TODO 定义用户模型， 分区用户，redis 存储 ， 集合 运算获取用户特征集合
////        将获取的数据交给LSTM + Transformer模型
////
//        try {
//            return ConnectionFactory.createConnection(conf);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

}
