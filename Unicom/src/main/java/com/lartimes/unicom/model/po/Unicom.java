package com.lartimes.unicom.model.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * <p>
 * 联通用户月表
 * </p>
 *
 * @author lartimes
 */
@Data
public class Unicom implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;
    private static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;
    private LocalDateTime timeNow;
    private String imsi;
    private String net;
    private String sex;
    private Integer ageWeight;
    private Integer arpu;
    private String brand;
    private String model;
    private Integer trafficWeight;
    private Integer callSum;
    private Integer smsTotal;

    public Unicom(String line) {
        String[] split = line.split(",");
//        201501,bdad30dc48d290b4e930293feea55dfd,2G,男,7,1,Apple,iPhone3GS,1,0,0
        this.timeNow = convert(split[0]);
        this.imsi = split[1];
        this.net = split[2];
        this.sex = split[3];
        this.ageWeight = Integer.parseInt(split[4]);
        this.arpu = Integer.parseInt(split[5]);
        this.brand = split[6];
        this.model = split[7];
        this.trafficWeight = Integer.parseInt(split[8]);
        this.callSum = Integer.parseInt(split[9]);
        this.smsTotal = Integer.parseInt(split[10]);
    }

    public static LocalDateTime convert(String input) {
        return LocalDate.parse(input + "01", dateFormatter).atStartOfDay();
    }
}
