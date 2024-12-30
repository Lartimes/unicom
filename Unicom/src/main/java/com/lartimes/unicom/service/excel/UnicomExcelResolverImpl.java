package com.lartimes.unicom.service.excel;

import cn.idev.excel.context.AnalysisContext;
import com.lartimes.unicom.model.po.ExcelUnicom;
import com.lartimes.unicom.storage.CsvNameHolder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 14:58
 */
//不可被spring 管理
@Slf4j
public class UnicomExcelResolverImpl implements ExcelSolverParent<ExcelUnicom> {
    private static final int BATCH_SIZE = 20000;
    private static final HikariDataSource dataSource;
    private static final String CREATE_TABLE = """
                     create table if not exists unicom_%S
            (
                id             bigint auto_increment,
                time_now       datetime     null,
                imsi           varchar(255) null,
                net            char(3)      null,
                sex            char(2)      null default '男',
                age_weight     int          null default 0,
                arpu           int          null default 0,
                brand          varchar(255) null,
                model          varchar(255) null default 0,
                traffic_weight int          null default 0,
                call_sum  int          null default 0,
                sms_total     int          null default 0,
                constraint unicom_%s_pk
                    primary key (id)
            )
                comment '联通用户月表';
            """;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/unicom");
        config.setUsername("root");
        config.setPassword("307314");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        dataSource = new HikariDataSource(config);
    }

    private final List<ExcelUnicom> cachedDataList = new ArrayList<>(BATCH_SIZE);

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void invoke(ExcelUnicom data, AnalysisContext analysisContext) {
        cachedDataList.add(data);
        if (data.getArpu() == null) {
            data.setArpu(0);
        }
        if (data.getAgeWeight() == null) {
            data.setAgeWeight(0);
        }
        if (data.getTrafficWeight() == null) {
            data.setTrafficWeight(0);
        }

        if (cachedDataList.size() >= BATCH_SIZE) {
            log.info("进行插入操作");
            processBatch(cachedDataList);
            cachedDataList.clear();
            log.info("插入2w条成功");
        }
    }

    private void processBatch(List<ExcelUnicom> cachedDataList) {
        String table = CsvNameHolder.get();
        try (Connection connection = getConnection();
             PreparedStatement ps = connection.prepareStatement(
                     "insert into unicom_" + table + " values (null , ? , ? , ? ,? , ? , ?,? , ? , ?,? , ? )");
             Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE.formatted(table , table));
            for (ExcelUnicom data : cachedDataList) {
                try {
                    ps.setObject(1, data.getTimeNow());
                    ps.setString(2, data.getImsi());
                    ps.setString(3, data.getNet());
                    ps.setString(4, data.getSex());
                    ps.setInt(5, data.getAgeWeight());
                    ps.setInt(6, data.getArpu());
                    ps.setString(7, data.getBrand());
                    ps.setString(8, data.getModel());
                    ps.setInt(9, data.getTrafficWeight());
                    ps.setInt(10, data.getCallSum());
                    ps.setInt(11, data.getSmsTotal());
                    ps.addBatch();
                } catch (Exception ignore) {
                }
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void doAfterAllAnalysed(AnalysisContext analysisContext) {
        if (!cachedDataList.isEmpty()) {
            processBatch(cachedDataList);
            cachedDataList.clear();
        }
        CsvNameHolder.remove();
        log.info("处理完成 : {}", analysisContext.readSheetHolder());
//        TODO 进行db 数据清洗 bgclean

//
    }
}
