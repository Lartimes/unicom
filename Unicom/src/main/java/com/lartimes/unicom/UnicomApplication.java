package com.lartimes.unicom;

import com.lartimes.unicom.mapreduce.driver.MysqlDBReaderDriverV2;
import com.lartimes.unicom.mapreduce.driver.UnicomDriver;
import com.lartimes.unicom.mapreduce.driver.UnicomGroupDriver;
import com.lartimes.unicom.mapreduce.jobs.UnicomCleanJobs;
import com.lartimes.unicom.mapreduce.jobs.UnicomPhoneReplaceJobs;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication


@EnableCaching
@EnableScheduling
@ConfigurationPropertiesScan("com.lartimes.unicom.conf")
@MapperScan(basePackages = "com.lartimes.unicom.mapper")
public class UnicomApplication implements ApplicationRunner {

    @Autowired
    private UnicomDriver unicomDriver;
    @Autowired
    private UnicomCleanJobs unicomCleanJobs;

    @Autowired
    private MysqlDBReaderDriverV2 mysqlDBReaderDriverV2;

    @Autowired
    private UnicomPhoneReplaceJobs unicomPhoneReplaceJobs;

//    @Autowired
//    private HBaseUtils hBaseUtils;

    @Autowired
    private UnicomGroupDriver unicomGroupDriver;

    public static void main(String[] args) {
        SpringApplication.run(UnicomApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        int i = unicomDriver.doJob(null);
//        System.out.println(i);
//        mysqlDBReaderDriverV2.doReader();
        unicomGroupDriver.doJob(null);
//        unicomPhoneReplaceJobs.cleanJobs(null , null , null);
//        hBaseUtils.batchInsert();
//        unicomCleanJobs.cleanJobs(null , null , null);
//        File file = new File("data");
//        long now = System.currentTimeMillis();
//        for (File listFile : Objects.requireNonNull(file.listFiles())) {
//            String name = listFile.getName();
//            name = name.substring(4, name.length() - ".csv".length());
//            System.out.println(name);
//            CsvNameHolder.set(name);
//            UnicomExcelResolverImpl unicomExcelResolver = ExcelSolverParent.unicomExcelResolver;
//            FastExcel.read(file, ExcelUnicom.class, unicomExcelResolver).sheet().doRead();
//        }
//        long then = System.currentTimeMillis();
//        System.out.print("用时:");
//        System.out.println((then - now) / 1000L);
    }
}
