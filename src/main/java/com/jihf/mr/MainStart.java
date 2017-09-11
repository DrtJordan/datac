package com.jihf.mr;

import com.jihf.mr.mapReduce.*;
import org.apache.hadoop.util.ProgramDriver;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-16 17:02
 * Mail：jihaifeng@raiyi.com
 */
public class MainStart {
    public static void main(String[] args) {
        int exitCode = -1;
        ProgramDriver driver = new ProgramDriver();
        try {
            driver.addClass("copyFile", CopyFile.class, "copyFile from HDFS.");
            //  hadoop jar datac-1.7.jar  compareWithMblDpi 移网DPI数据作为输入目录org1 投诉样本号码数据作为输入目录org2  匹配投诉号码的结果输出目录
            driver.addClass("compareWithMblDpi", CompareWithMblDpi.class, "compare phoneMd5 with MblDpi.");
            // hadoop jar datac-1.7.jar  complainHostSort 匹配投诉号码输出的结果目录 计数后的输出目录
            driver.addClass("complainHostSort", ComplainHostSort.class, "count the num of host visited in mobile dpi.");
            // hadoop jar datac-1.7.jar  matchMblComplain <mblCdrFolder mblDpiFolder outputFolder>
            /*
              msisdn|imei|imsi|callDurationTotal|callDurationMax|cdrDataCount|dpiDataCount|score
              手机号MD5|设备imei|设备imsi|投诉电话总时长|投诉电话最大时长|投诉电话次数|投诉域名访问次数|投诉电话和投诉域名的总得分(S=A*x+B*y+(5-C)*z 中的 A*x+B*y)
             */
            driver.addClass("matchMblComplain", MatchMblComplain.class, "count the num of host visited in mobile dpi.");
            // hadoop jar datac-1.7.jar  mblDpiHostSort <mblDpiFolder outputFolder>
            /*
            hostVisitTimes|host
             */
            driver.addClass("mblDpiHostSort", MblDpiHostSort.class, "count the num of host visited in mobile dpi.");
            // hadoop jar datac-1.7.jar  matchMblCdrComplain <mblCdrFolder outputFolder>

            driver.addClass("sortComplainUserHost", SortComplainUserHost.class, "aaa");
            driver.addClass("sortData", SortData.class, "aaa");
            driver.addClass("matchDpiHostWithChe", MatchDpiHostWithChe.class, "aaa");
            driver.addClass("matchMblHostComplain", MatchMblHostComplain.class, "aaa");
            // 投诉用户中拨打投诉电话的
            driver.addClass("matchMblCdrComplain", MatchMblCdrComplain.class, "filter the complain phoneNum from mobile data.");
            // hadoop jar datac-1.9.jar   fixDpiPhoneNum  <input>  <output>
            driver.addClass("fixDpiPhoneNum", FixDpiPhoneNum.class, "filter the phoneNum from fixDpi data.");
            driver.addClass("fixCdrComplain", FixCdrComplain.class, "filter the complain phoneNum from fixCdr data.");
            driver.addClass("flowData", FlowData.class, "aaaa");
            driver.addClass("mblDpiPhoneMd5", MblDpiPhoneMd5.class, "filter the phone md5 from mobile Dpi data.");
            driver.addClass("countData", CountData.class, "count data times.");
            exitCode = driver.run(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.exit(exitCode);
    }
}
