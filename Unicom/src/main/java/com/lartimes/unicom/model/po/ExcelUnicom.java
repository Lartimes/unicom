package com.lartimes.unicom.model.po;

import cn.idev.excel.annotation.ExcelProperty;
import cn.idev.excel.converters.integer.IntegerStringConverter;
import com.lartimes.unicom.service.converter.StringMappingIntConverter;
import com.lartimes.unicom.service.converter.StringToLocalDateTimeConverter;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 13:46
 */
@Data
public class ExcelUnicom {

    @ExcelProperty(value = "月份",
            converter = StringToLocalDateTimeConverter.class
    )
    private LocalDateTime timeNow;
    @ExcelProperty("IMSI")
    private String imsi;
    @ExcelProperty("网别")
    private String net;
    @ExcelProperty("性别")
    private String sex;
    @ExcelProperty(value = "年龄值段" , converter = StringMappingIntConverter.class)
    private Integer ageWeight;
    @ExcelProperty(value = "ARPU值段" , converter = StringMappingIntConverter.class)
    private Integer arpu;

    @ExcelProperty("终端品牌")
    private String brand;
    @ExcelProperty("终端型号")
    private String model;
    @ExcelProperty(value = "流量使用量" , converter = StringMappingIntConverter.class)
    private Integer trafficWeight;
    @ExcelProperty(value = "语音通话时长", converter = IntegerStringConverter.class)
    private Integer callSum;
    @ExcelProperty(value = "短信条数", converter = IntegerStringConverter.class)
    private Integer smsTotal;
}
