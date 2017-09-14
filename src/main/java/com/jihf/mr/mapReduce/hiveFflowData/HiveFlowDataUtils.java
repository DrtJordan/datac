package com.jihf.mr.mapReduce.hiveFflowData;

import com.jihf.mr.utils.StringUtils;

import java.util.HashMap;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-08 13:49
 * Mail：jihaifeng@raiyi.com
 */
public class HiveFlowDataUtils {

    public static HiveFlowDataBean parse2FlowBean(String... str) {
        if (null == str || str.length < 35) {
            return null;
        }
        HiveFlowDataBean dataBean = new HiveFlowDataBean();

        dataBean.uid = StringUtils.strIsEmpty(str[0]) ? null : str[0];
        dataBean.mobile = StringUtils.strIsEmpty(str[1]) ? null : str[1];
        dataBean.operators = StringUtils.strIsEmpty(str[2]) ? null : str[2];
        dataBean.province_id = StringUtils.strIsEmpty(str[3]) ? null : str[3];
        dataBean.province_code = StringUtils.strIsEmpty(str[4]) ? null : str[4];
        dataBean.city_id = StringUtils.strIsEmpty(str[5]) ? null : str[5];
        dataBean.city_code = StringUtils.strIsEmpty(str[6]) ? null : str[6];
        dataBean.date = StringUtils.strIsEmpty(str[7]) ? null : str[7];
        dataBean.query_time = StringUtils.strIsEmpty(str[8]) ? null : str[8];
        dataBean.net_type = StringUtils.strIsEmpty(str[9]) ? null : str[9];
        dataBean.main_price = StringUtils.strIsEmpty(str[10]) ? null : str[10];
        dataBean.main_name = StringUtils.strIsEmpty(str[11]) ? null : str[11];
        dataBean.has_other_pkt = StringUtils.strIsEmpty(str[12]) ? null : str[12];
        dataBean.flow_total = StringUtils.strIsEmpty(str[13]) ? null : str[13];
        dataBean.flow_used = StringUtils.strIsEmpty(str[14]) ? null : str[14];
        dataBean.flow_left = StringUtils.strIsEmpty(str[15]) ? null : str[15];
        dataBean.flow_over = StringUtils.strIsEmpty(str[16]) ? null : str[16];
        dataBean.flow_carry = StringUtils.strIsEmpty(str[17]) ? null : str[17];
        dataBean.voice_total = StringUtils.strIsEmpty(str[18]) ? null : str[18];
        dataBean.voice_used = StringUtils.strIsEmpty(str[19]) ? null : str[19];
        dataBean.voice_left = StringUtils.strIsEmpty(str[20]) ? null : str[20];
        dataBean.voice_over = StringUtils.strIsEmpty(str[21]) ? null : str[21];
        dataBean.sms_total = StringUtils.strIsEmpty(str[22]) ? null : str[22];
        dataBean.sms_used = StringUtils.strIsEmpty(str[23]) ? null : str[23];
        dataBean.sms_left = StringUtils.strIsEmpty(str[24]) ? null : str[24];
        dataBean.sms_over = StringUtils.strIsEmpty(str[25]) ? null : str[25];
        dataBean.pkg_infos = StringUtils.strIsEmpty(str[26]) ? null : str[26];
        dataBean.extradata = StringUtils.strIsEmpty(str[27]) ? null : str[27];
        dataBean.query_id = StringUtils.strIsEmpty(str[28]) ? null : str[28];
        dataBean.query_consumer_id = StringUtils.strIsEmpty(str[29]) ? null : str[29];
        dataBean.querylevel = StringUtils.strIsEmpty(str[30]) ? null : str[30];
        dataBean.query_channel = StringUtils.strIsEmpty(str[31]) ? null : str[31];
        dataBean.query_rsp = StringUtils.strIsEmpty(str[32]) ? null : str[32];
        dataBean.source = StringUtils.strIsEmpty(str[33]) ? null : str[33];
        dataBean.source_tag = StringUtils.strIsEmpty(str[34]) ? null : str[34];
        return dataBean;
    }
}
