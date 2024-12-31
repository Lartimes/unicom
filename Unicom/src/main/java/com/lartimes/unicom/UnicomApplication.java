package com.lartimes.unicom;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication

@MapperScan(basePackages = "com.lartimes.unicom.mapper")
public class UnicomApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(UnicomApplication.class, args);
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
//        File file = new File("src/main/resources/数据大赛201601.csv");
//        CsvNameHolder.set("201601");
//        UnicomExcelResolverImpl unicomExcelResolver = ExcelSolverParent.unicomExcelResolver;
//        FastExcel.read(file, ExcelUnicom.class, unicomExcelResolver).sheet().doRead();
    }
}
