package com.lartimes.unicom.model.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

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


}
