package com.jihf.mr;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-10 16:25
 * Mail：jihaifeng@raiyi.com
 */
public class UserBean {
    public String msisdn;// 用户手机号码         对应字符串的位置：2
    public String imei;// 移动台设备标识         对应字符串的位置：3
    public String startTime;// 业务流开始时间    对应字符串的位置：18
    public String endTime;//业务流结束时间       对应字符串的位置：19
    public String duration;//业务流结束时间       对应字符串的位置：20
    public String recordCloseCause; //记录关闭原因  对应字符串的位置：28
    public String userAgent;// User Agent信息      对应字符串的位置：29
    public String destinationURL;//用户访问的目标网站的URL 对应字符串的位置：30
    public String domainName;//外部网站的域名       对应字符串的位置：31


}
