package com.jihf.mr;

import com.jihf.mr.mapReduce.*;
import com.jihf.mr.mapReduce.complainModel.*;
import com.jihf.mr.mapReduce.zaojiao.MatchDpiHostWithZaoJiao;
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

            // hadoop jar datac-1.7.jar  mblDpiHostSort <mblDpiInput resultOutput 【choose：outputDataLength minVisitTimes】>
            // out：hostVisitTimes|host
            driver.addClass("mblDpiHostSort", MblDpiHostSort.class, "count the num of host visited in mobile dpi.");

            //  hadoop jar datac-1.7.jar  compareWithMblDpi 移网DPI数据作为输入目录org1 投诉样本号码数据作为输入目录org2  匹配投诉号码的结果输出目录
            // out：phoneNum|url
            driver.addClass("compareWithMblDpi", CompareWithMblDpi.class, "compare phoneMd5 with MblDpi.");

            // hadoop jar datac-1.7.jar  matchMblComplain <mblCdrFolder mblDpiFolder outputFolder>
            // msisdn|imei|imsi|callDurationTotal|callDurationMax|cdrDataCount|dpiDataCount|score
            // 手机号MD5|设备imei|设备imsi|投诉电话总时长|投诉电话最大时长|投诉电话次数|投诉域名访问次数|投诉电话和投诉域名的总得分(S=A*x+B*y+(5-C)*z 中的 A*x+B*y)
            driver.addClass("matchMblComplain", MatchMblComplain.class, "count the num of host visited in mobile dpi.");

            // hadoop jar datac-1.7.jar  matchDpiHostWithChe  dpi数据目录 程序结果输出目录  dpi数据类型（1移网dpi，固网dpi） dpi数据格式（1压缩的二进制，2Text文本）
            // out：手机号|url|ua
            driver.addClass("matchDpiHostWithChe", MatchDpiHostWithChe.class, "matchDpiHostWithChe");

            // hadoop jar datac-1.7.jar matchMblHostComplain 移网DPI数据目录 程序结果输出目录
            // out：投诉域名|访问次数
            driver.addClass("matchMblHostComplain", MatchMblHostComplain.class, "matchMblHostComplain");

            // hadoop  jar  datac-1.7.jar  matchMblCdrComplain  移网语音数据 投诉样本数据  程序结果输出目录
            // out：手机号|投诉号码|语音总时长|语音最大时长|语音次数|得分
            driver.addClass("matchMblCdrComplain", MatchMblCdrComplain.class, "filter the complain phoneNum from mobile data.");

            // hadoop jar datac-1.9.jar   fixDpiPhoneNum  <input>  <output>
            // out：手机号|ua
            driver.addClass("fixDpiPhoneNum", FixDpiPhoneNum.class, "filter the phoneNum from fixDpi data.");
            // hadoop jar datac-1.9.jar   fixCdrComplain  <input>  <output>
            // out：手机号|拨打类型|msisdn|语音时长
            driver.addClass("fixCdrComplain", FixCdrComplain.class, "filter the complain phoneNum from fixCdr data.");

            //  hadoop jar datac-1.16-shaded.jar  complainData <流量数据  短信数据  电话数据  输出目录>
            // out：手机号|…过程值…|得分
            driver.addClass("complainData",ComplainData.class,"complain data");


            driver.addClass("flowData", FlowData.class, "flow data");
            driver.addClass("smsData",SmsData.class,"sms data");
            driver.addClass("etlData", EtlData.class, "etl data");

            //  hadoop jar datac-1.16-shaded.jar  complainData <样本手机号  流量数据  短信数据  电话数据  输出目录>
            driver.addClass("matchComplainData",MatchComplainData.class,"complain data");

            //  hadoop jar datac-1.16-shaded.jar  matchDpiHostWithZaoJiao <移网dpi数据  移网cdr数据>
            driver.addClass("matchDpiHostWithZaoJiao", MatchDpiHostWithZaoJiao.class,"zaojiao Data");

            driver.addClass("mblDpiPhoneMd5", MblDpiPhoneMd5.class, "filter the phone md5 from mobile Dpi data.");
            driver.addClass("copyFile", CopyFile.class, "copyFile from HDFS.");
            // hadoop jar datac-1.7.jar  complainHostSort 匹配投诉号码输出的结果目录 计数后的输出目录
            driver.addClass("complainHostSort", ComplainHostSort.class, "count the num of host visited in mobile dpi.");
            // hadoop jar datac-1.7.jar  matchMblCdrComplain <mblCdrFolder outputFolder>
            driver.addClass("sortComplainUserHost", SortComplainUserHost.class, "complain sort");

            driver.addClass("sortData", SortData.class, "sort");

            exitCode = driver.run(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.exit(exitCode);
    }
}
