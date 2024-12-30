package com.lartimes.unicom.model.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/**
 * <p>
 * 字段数据字典表
 * </p>
 *
 * @author lartimes
 */
@Data
@TableName("dictionary")
public class Dictionary implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    @TableField( "`column`")
    private String column;

    private Integer weight;
    @TableField( "`range`")
    private String range;


}
