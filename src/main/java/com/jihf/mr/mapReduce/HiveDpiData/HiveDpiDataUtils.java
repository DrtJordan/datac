package com.jihf.mr.mapReduce.HiveDpiData;

import com.jihf.mr.utils.StringUtils;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-15 13:42
 * Mail：jihaifeng@raiyi.com
 */
public class HiveDpiDataUtils {
    public static HiveDpiDataBean parse2DpiData(String... str) {
        if (null == str || str.length < 8) {
            return null;
        }
        HiveDpiDataBean dataBean = new HiveDpiDataBean();

        dataBean.phone_number = StringUtils.strIsEmpty(str[0]) ? null : str[0];
        dataBean.device_id = StringUtils.strIsEmpty(str[1]) ? null : str[1];
        dataBean.province = StringUtils.strIsEmpty(str[2]) ? null : str[2];
        dataBean.log_date = StringUtils.strIsEmpty(str[3]) ? null : str[3];
        dataBean.user_agent = StringUtils.strIsEmpty(str[4]) ? null : str[4];
        dataBean.host_freq = StringUtils.strIsEmpty(str[5]) ? null : str[5];
        dataBean.province_code = StringUtils.strIsEmpty(str[6]) ? null : str[6];
        dataBean.day = StringUtils.strIsEmpty(str[7]) ? null : str[7];

        return dataBean;
    }
}