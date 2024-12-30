package com.lartimes.unicom.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.lartimes.unicom.model.po.Dictionary;
import org.apache.ibatis.annotations.Mapper;

/**
 * <p>
 * 字段数据字典表 Mapper 接口
 * </p>
 *
 * @author lartimes
 */
@Mapper
public interface DictionaryMapper extends BaseMapper<Dictionary> {

}
