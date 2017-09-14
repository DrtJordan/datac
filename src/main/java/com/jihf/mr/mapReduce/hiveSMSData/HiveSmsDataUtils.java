package com.jihf.mr.mapReduce.hiveSMSData;

import com.jihf.mr.utils.StringUtils;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-08 13:49
 * Mail：jihaifeng@raiyi.com
 */
public class HiveSmsDataUtils {

    public static HiveSmsDataBean parse2SmsBean(String... str) {
        if (null == str || str.length < 28) {
            return null;
        }
        HiveSmsDataBean dataBean = new HiveSmsDataBean();

        dataBean.uid = StringUtils.strIsEmpty(str[0]) ? null : str[0];
        dataBean.mobile = StringUtils.strIsEmpty(str[1]) ? null : str[1];
        dataBean.operators = StringUtils.strIsEmpty(str[2]) ? null : str[2];
        dataBean.province_id = StringUtils.strIsEmpty(str[3]) ? null : str[3];
        dataBean.province_code = StringUtils.strIsEmpty(str[4]) ? null : str[4];
        dataBean.city_id = StringUtils.strIsEmpty(str[5]) ? null : str[5];
        dataBean.city_code = StringUtils.strIsEmpty(str[6]) ? null : str[6];
        dataBean.consumer_id = StringUtils.strIsEmpty(str[7]) ? null : str[7];
        dataBean.msg_count = StringUtils.strIsEmpty(str[8]) ? null : str[8];
        dataBean.req_msg_body = StringUtils.strIsEmpty(str[9]) ? null : str[9];
        dataBean.send_msg_body = StringUtils.strIsEmpty(str[10]) ? null : str[10];
        dataBean.accept_type = StringUtils.strIsEmpty(str[11]) ? null : str[11];
        dataBean.send_code = StringUtils.strIsEmpty(str[12]) ? null : str[12];
        dataBean.msg_id = StringUtils.strIsEmpty(str[13]) ? null : str[13];
        dataBean.bulk_msg_id = StringUtils.strIsEmpty(str[14]) ? null : str[14];
        dataBean.user_msg_id = StringUtils.strIsEmpty(str[15]) ? null : str[15];
        dataBean.user_bulk_msg_id = StringUtils.strIsEmpty(str[16]) ? null : str[16];
        dataBean.channel_msg_id = StringUtils.strIsEmpty(str[17]) ? null : str[17];
        dataBean.channel_id = StringUtils.strIsEmpty(str[18]) ? null : str[18];
        dataBean.file_id = StringUtils.strIsEmpty(str[19]) ? null : str[19];
        dataBean.sms_code = StringUtils.strIsEmpty(str[20]) ? null : str[20];
        dataBean.sms_status = StringUtils.strIsEmpty(str[21]) ? null : str[21];
        dataBean.error_msg = StringUtils.strIsEmpty(str[22]) ? null : str[22];
        dataBean.acc_time = StringUtils.strIsEmpty(str[23]) ? null : str[23];
        dataBean.send_time = StringUtils.strIsEmpty(str[24]) ? null : str[24];
        dataBean.notify_time = StringUtils.strIsEmpty(str[25]) ? null : str[25];
        dataBean.sms_type = StringUtils.strIsEmpty(str[26]) ? null : str[26];
        dataBean.source = StringUtils.strIsEmpty(str[27]) ? null : str[27];
        return dataBean;
    }
}
