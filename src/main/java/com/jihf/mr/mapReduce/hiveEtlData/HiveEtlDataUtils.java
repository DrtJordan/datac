package com.jihf.mr.mapReduce.hiveEtlData;

import com.jihf.mr.utils.StringUtils;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-08 13:49
 * Mail：jihaifeng@raiyi.com
 */
public class HiveEtlDataUtils {

    public static HiveEtlDataBean parse2EtlAvro(String... str) {
        if (null == str || str.length < 38) {
            return null;
        }
        HiveEtlDataBean dataBean = new HiveEtlDataBean();

        dataBean.mobile = StringUtils.strIsEmpty(str[0]) ? null : str[0];
        dataBean.device_id = StringUtils.strIsEmpty(str[1]) ? null : str[1];
        dataBean.mobile_md5 = StringUtils.strIsEmpty(str[2]) ? null : str[2];
        dataBean.mobile_imei = StringUtils.strIsEmpty(str[3]) ? null : str[3];
        dataBean.province = StringUtils.strIsEmpty(str[4]) ? null : str[4];
        dataBean.province_name = StringUtils.strIsEmpty(str[5]) ? null : str[5];
        dataBean.city = StringUtils.strIsEmpty(str[6]) ? null : str[6];
        dataBean.city_name = StringUtils.strIsEmpty(str[7]) ? null : str[7];
        dataBean.create_date = StringUtils.strIsEmpty(str[8]) ? null : str[8];
        dataBean.rowversion = StringUtils.strIsEmpty(str[9]) ? null : str[9];
        dataBean.query_date = StringUtils.strIsEmpty(str[10]) ? null : str[10];
        dataBean.lt_query_date = StringUtils.strIsEmpty(str[11]) ? null : str[11];
        dataBean.rtb_key = StringUtils.strIsEmpty(str[12]) ? null : str[12];
        dataBean.rtb_province = StringUtils.strIsEmpty(str[13]) ? null : str[13];
        dataBean.log_date = StringUtils.strIsEmpty(str[14]) ? null : str[14];
        dataBean.flow_score = StringUtils.strIsEmpty(str[15]) ? null : str[15];
        dataBean.net_days = StringUtils.strIsEmpty(str[16]) ? null : str[16];
        dataBean.net_counts = StringUtils.strIsEmpty(str[17]) ? null : str[17];
        dataBean.net_duration = StringUtils.strIsEmpty(str[18]) ? null : str[18];
        dataBean.basic_fee = StringUtils.strIsEmpty(str[19]) ? null : str[19];
        dataBean.visit_area = StringUtils.strIsEmpty(str[20]) ? null : str[20];
        dataBean.basic_fee_over_daily = StringUtils.strIsEmpty(str[21]) ? null : str[21];
        dataBean.roaming_num_count = StringUtils.strIsEmpty(str[22]) ? null : str[22];
        dataBean.roaming_flow_ratio = StringUtils.strIsEmpty(str[23]) ? null : str[23];
        dataBean.complaint_ratio = StringUtils.strIsEmpty(str[24]) ? null : str[24];
        dataBean.max_calltime_ratio = StringUtils.strIsEmpty(str[25]) ? null : str[25];
        dataBean.is_call_times_normal = StringUtils.strIsEmpty(str[26]) ? null : str[26];
        dataBean.qmd = StringUtils.strIsEmpty(str[27]) ? null : str[27];
        dataBean.result = StringUtils.strIsEmpty(str[28]) ? null : str[28];
        dataBean.s_table = StringUtils.strIsEmpty(str[29]) ? null : str[29];
        dataBean.fee_add = StringUtils.strIsEmpty(str[30]) ? null : str[30];
        dataBean.basicfee_area = StringUtils.strIsEmpty(str[31]) ? null : str[31];
        dataBean.op = StringUtils.strIsEmpty(str[32]) ? null : str[32];
        dataBean.flow_ratio = StringUtils.strIsEmpty(str[33]) ? null : str[33];
        dataBean.flow_over = StringUtils.strIsEmpty(str[34]) ? null : str[34];
        dataBean.id = StringUtils.strIsEmpty(str[35]) ? null : str[35];
        dataBean.flow_size = StringUtils.strIsEmpty(str[36]) ? null : str[36];
        dataBean.flow_use = StringUtils.strIsEmpty(str[37]) ? null : str[37];
        dataBean.last_mydate_visitarea = StringUtils.strIsEmpty(str[38]) ? null : str[38];
        dataBean.province_my_area = StringUtils.strIsEmpty(str[39]) ? null : str[39];
        return dataBean;
    }
}
