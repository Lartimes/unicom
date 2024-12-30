package com.lartimes.unicom.service.excel;

import cn.idev.excel.read.listener.ReadListener;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 13:39
 */
public interface ExcelSolverParent<T> extends ReadListener<T> {
    UnicomExcelResolverImpl unicomExcelResolver = new UnicomExcelResolverImpl();
}
