package com.lartimes.unicom.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lartimes.unicom.model.po.Dictionary;
import com.lartimes.unicom.service.DictionaryService;
import jakarta.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 14:00
 */
@Slf4j
@Repository
public class DictionaryStorage implements BeanPostProcessor {
    private static boolean flag = false;
    //    dictionary
//    phone model brand
    private final DictionaryService dictionaryService;
    @Nullable
    public Map<String, Map<String, Integer>> dictionaryMap;
    @Autowired
    private DataSource dataSource;

    public DictionaryStorage(DictionaryService dictionaryService) {
        this.dictionaryService = dictionaryService;
    }

    @SneakyThrows
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        List<Dictionary> list = dictionaryService.list();
        if (list.isEmpty()) {
            return bean;
        }
        this.dictionaryMap = list.stream()
                .collect(Collectors.groupingBy(
                        Dictionary::getColumn,
                        Collectors.toMap(Dictionary::getRange, Dictionary::getWeight, (existing, replacement) -> existing)
                ));
        if (this.dictionaryMap.isEmpty()) {
            dictionaryMap = Collections.emptyMap();
        }
//        将这些map 传入文件
//        在读取出来
        String parentPath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("")).toURI().toString().substring("file:_".length());
        if (!flag) {
            try {
                List<String> tableNames = new ArrayList<>();
                try (Connection connection = dataSource.getConnection()) {
                    DatabaseMetaData metaData = connection.getMetaData();
                    String[] types = {"TABLE"};
                    try (ResultSet rs = metaData.getTables(null, null, "%", types)) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            tableNames.add(tableName);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    // 处理异常
                }
                tableNames = tableNames.stream().filter(tableName -> tableName.contains("unicom")).toList();
                HashMap<String, List<String>> tableMap = new HashMap<>();
                tableMap.put("tables", tableNames);
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.writeValue(new File(parentPath + "tables.json"), tableMap);
                objectMapper.writeValue(new File(parentPath + "dictionary.json"), dictionaryMap);
                log.info("数据成功写入 dictionary.json 文件");
                log.info("数据成功写入 tables.json 文件");
                flag = true;
            } catch (IOException e) {
                log.info("写入失败");
            }
        }
        return bean;
    }
}
