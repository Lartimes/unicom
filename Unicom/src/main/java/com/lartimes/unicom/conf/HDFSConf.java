package com.lartimes.unicom.conf;

import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/4 13:51
 */
@Configuration
public class HDFSConf {
    private static org.apache.hadoop.conf.Configuration conf = null;
    private static FileSystem fs = null;
    /**
     * hdfs 服务器地址
     **/
    @Value("${hadoop.hdfs.hostname}")
    private String hostname;
    /**
     * hdfs 服务器端口
     **/
    @Value("${hadoop.hdfs.port}")
    private String port;
    /**
     * hdfs 服务器账户
     **/
    @Value("${hadoop.hdfs.username}")
    private String username;

    @Bean
    @SneakyThrows
    public FileSystem fileSystem() {
        System.setProperty("HADOOP_USER_NAME", "root");
        conf = new org.apache.hadoop.conf.Configuration();
        final String uri = "hdfs://" + hostname + ":" + port;
        conf.set("fs.defaultFS", uri);
        conf.set("dfs.client.use.datanode.hostname", "true");
        fs = FileSystem.get(conf);
        return fs;
    }

}
