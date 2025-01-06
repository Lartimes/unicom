package com.lartimes.unicom.utils;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2025/1/6 22:54
 */
@Repository
public class HBaseUtils {
    @Autowired
    private Connection connection;

//    进行插入 根据行插入
//    根据行键查询
}
