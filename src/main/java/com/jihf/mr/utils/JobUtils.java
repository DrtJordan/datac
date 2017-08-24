package com.jihf.mr.utils;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-11 09:11
 * Mail：jihaifeng@raiyi.com
 */
public class JobUtils {
    // 异常退出
    public static void exit(String msg) {
        System.out.println(StringUtils.strIsEmpty(msg) ? "抛出异常" : msg);
        System.exit(0);
    }
}
