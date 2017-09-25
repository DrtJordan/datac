package com.jihf.mr.utils;

import org.apache.hadoop.io.Text;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-29 11:23
 * Mail：jihaifeng@raiyi.com
 */
public class DpiDataUtils {
    /**
     * 输入数据转数组
     *
     * @param value     输入数据
     * @param minLength 数据最小长度
     * @return 数组数据
     */
    public static String[] getSplitData(Text value, int minLength) {
        if (null == value){
            return null;
        }
        String data = value.toString();
        if (StringUtils.strIsEmpty(data)) {
            JobUtils.exit("input data is null.");
            return null;
        }

        String[] datas = data.split("\\|", -1);
        if (datas.length < minLength) {
            JobUtils.exit("input data has missed some data.");
            return null;
        }
        return datas;
    }
}
