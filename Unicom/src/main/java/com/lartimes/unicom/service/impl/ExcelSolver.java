package com.lartimes.unicom.service.impl;

import cn.idev.excel.FastExcel;
import com.lartimes.unicom.model.po.ExcelUnicom;
import com.lartimes.unicom.service.excel.ExcelSolverParent;
import com.lartimes.unicom.service.excel.UnicomExcelResolverImpl;
import com.lartimes.unicom.storage.CsvNameHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Objects;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 20:19
 */
@Service
@Slf4j
public class ExcelSolver {

    /**
     * @param dir csv 目录
     */
    public void importCSVToDb(String dir) {
        UnicomExcelResolverImpl unicomExcelResolver = ExcelSolverParent.unicomExcelResolver;
        File file = new File(dir);
        if (!file.exists()) {
            log.error("不存在该目录");
            return;
        }
        //    map + strategy 后续再更改代码，目前先完成
//        这里截取table 名字也需要动态引入， 策略模式等
//       TODO 线程池 ， future ，异步等
//        使用回调机制 /使用 CompletableFuture 不显示等待结果
        for (File listFile : Objects.requireNonNull(file.listFiles())) {
            String name = listFile.getName();
            name = name.substring(4, name.length() - ".csv".length());
            CsvNameHolder.set(name); //ThreadLocal截取日期
            FastExcel.read(listFile, ExcelUnicom.class, unicomExcelResolver).sheet().doRead();
            //进行处理映射逻辑
        }

    }
}
