package com.lartimes.unicom.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lartimes.unicom.model.po.Dictionary;
import com.lartimes.unicom.service.DictionaryService;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    public DictionaryStorage(DictionaryService dictionaryService) {
        this.dictionaryService = dictionaryService;
    }

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
        if (!flag) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                objectMapper.writeValue(new File("src/main/resources/dictionary.json"), dictionaryMap);
                log.info("数据成功写入 dictionary.json 文件");
                flag = true;
            } catch (IOException e) {
                log.info("写入失败");
            }
        }

        return bean;
    }
}
