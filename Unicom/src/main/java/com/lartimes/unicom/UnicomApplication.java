package com.lartimes.unicom;

import cn.idev.excel.FastExcel;
import com.lartimes.unicom.model.po.ExcelUnicom;
import com.lartimes.unicom.service.excel.ExcelSolverParent;
import com.lartimes.unicom.service.excel.UnicomExcelResolverImpl;
import com.lartimes.unicom.storage.CsvNameHolder;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.util.Objects;

@SpringBootApplication

@MapperScan(basePackages = "com.lartimes.unicom.mapper")
public class UnicomApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(UnicomApplication.class, args);
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {

        File file = new File("data");
        long now = System.currentTimeMillis();
        for (File listFile : Objects.requireNonNull(file.listFiles())) {
            String name = listFile.getName();
            name = name.substring(4 , name.length() - ".csv".length());
            System.out.println(name);
            CsvNameHolder.set(name);
            UnicomExcelResolverImpl unicomExcelResolver = ExcelSolverParent.unicomExcelResolver;
            FastExcel.read(file, ExcelUnicom.class, unicomExcelResolver).sheet().doRead();
        }
        long then = System.currentTimeMillis();
        System.out.print("用时:");
        System.out.println((then - now) /1000L  );
    }
}
