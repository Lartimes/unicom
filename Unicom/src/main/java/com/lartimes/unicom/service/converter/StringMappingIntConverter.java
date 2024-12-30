package com.lartimes.unicom.service.converter;

import cn.idev.excel.converters.Converter;
import cn.idev.excel.enums.CellDataTypeEnum;
import cn.idev.excel.metadata.GlobalConfiguration;
import cn.idev.excel.metadata.data.ReadCellData;
import cn.idev.excel.metadata.property.ExcelContentProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 14:03
 */
@Slf4j
public class StringMappingIntConverter implements Converter<Integer> {
    //    mapping
    private static final Map<String, String> MAP = Map.of("ageWeight", "年龄值段",
            "arpu", "ARPU值段", "trafficWeight", "流量使用量");
    private static Map<String, Map<String, Integer>> dictionaryMap = Collections.emptyMap();

    //    src/main/resources/dictionary.json
    static {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            dictionaryMap = objectMapper.readValue(
                    new File("src/main/resources/dictionary.json"),
                    new TypeReference<Map<String, Map<String, Integer>>>() {
                    }
            );
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public Class<?> supportJavaTypeKey() {
        return Integer.class;
    }

    @Override
    public CellDataTypeEnum supportExcelTypeKey() {
        return CellDataTypeEnum.STRING;
    }

    @Override
    public Integer convertToJavaData(ReadCellData<?> cellData, ExcelContentProperty contentProperty, GlobalConfiguration globalConfiguration) throws Exception {
        String key = contentProperty.getField().getName();
        String stringValue = cellData.getStringValue();
        if (Objects.requireNonNull(dictionaryMap).isEmpty()) {
            throw new RuntimeException("数据字典不存在，不能进行CSV转换");
        }
        return dictionaryMap.get(MAP.get(key)).getOrDefault(stringValue, 0);
    }

}
