package com.lartimes.unicom.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.lartimes.unicom.mapper.DictionaryMapper;
import com.lartimes.unicom.model.po.Dictionary;
import com.lartimes.unicom.service.DictionaryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 字段数据字典表 服务实现类
 * </p>
 *
 * @author lartimes
 */
@Slf4j
@Service
public class DictionaryServiceImpl extends ServiceImpl<DictionaryMapper, Dictionary>
        implements DictionaryService {

}
