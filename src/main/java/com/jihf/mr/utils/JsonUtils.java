package com.jihf.mr.utils;

import com.jihf.mr.UserBean;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-11 09:48
 * Mail：jihaifeng@raiyi.com
 */
public class JsonUtils {
    public static UserBean parseToBean(String[] datas) {

        UserBean userBean = new UserBean();
        userBean.msisdn = datas[1];
        userBean.imei = datas[2];
        userBean.startTime = datas[17];
        userBean.endTime = datas[18];
        userBean.duration = datas[19];
        userBean.recordCloseCause = datas[27];
        userBean.userAgent = datas[28];
        userBean.destinationURL = datas[29];
        userBean.domainName = datas[30];
        return userBean;
    }
}
