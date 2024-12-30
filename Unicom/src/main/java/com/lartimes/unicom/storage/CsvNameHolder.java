package com.lartimes.unicom.storage;

/**
 * @author wüsch
 * @version 1.0
 * @description:
 * @since 2024/12/29 16:08
 */
public class CsvNameHolder {
    private static final ThreadLocal<String> THREAD_LOCAL = new ThreadLocal<>();


    public static void set(Object id) {
        THREAD_LOCAL.set(String.valueOf(id));
    }

    public static String get() {
        return THREAD_LOCAL.get();
    }

    public static void remove() {
        THREAD_LOCAL.remove();
    }
}
