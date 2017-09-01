package com.jihf.mr.mapReduce.Mapper;

import com.jihf.mr.utils.DpiDataUtils;
import com.jihf.mr.utils.Matcher;
import com.jihf.mr.utils.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * make the MobileDpiData as the input data two
 */
public class MblDpiMapperForHost extends Mapper<LongWritable, Text, Text, Text> {
    private static Matcher matcher = new Matcher(true);
    private static List<String> matchHostList = new ArrayList<String>();
    private Text mapKey = new Text();
    private Text mapVal = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (matchHostList.size() == 0) {
            matchHostList.add("jbzs.12321.cn/12321SMSReport/");
            matchHostList.add("c.interface.gootion.com/ws/v2/numbermark");
            matchHostList.add("315.gov.cn");
            matchHostList.add("12315.cn");

        }
        for (int i = 0; i < matchHostList.size(); i++) {
            matcher.addPattern(matchHostList.get(i).toUpperCase(), i);
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] datas = DpiDataUtils.getSplitData(value, 40);
        if (null != datas) {
            String imsi = datas[0];
            String msisdn = datas[1];
            String imei = datas[2];
            String destination = datas[29];
            String domainName = datas[30];
            String host = datas[31];
            String hostComplain = null;

            Matcher.MatchResult[] matchResults = null;


            if (!StringUtils.strIsEmpty(destination)) {
                // 获取 destination带path值 ,去掉参数
                int index = destination.indexOf("HTTP://");
                if (index != -1) {
                    destination = destination.substring(index + 7);
                }
                index = destination.indexOf("?");
                if (index != -1) {
                    destination = destination.substring(0, index);
                }
                matchResults = matcher.match(destination);
            }

            if ((null == matchResults || matchResults.length == 0) && !StringUtils.strIsEmpty(domainName)) {
                matchResults = matcher.match(domainName);
            }

            if ((null == matchResults || matchResults.length == 0) && !StringUtils.strIsEmpty(host)) {
                matchResults = matcher.match(host);
            }
            if (null != matchResults && matchResults.length != 0) {
                for (Matcher.MatchResult result : matchResults) {
                    String complainHost = result.pattern;
                    if (!StringUtils.strIsEmpty(complainHost)) {
                        mapKey.set(msisdn);
                        mapVal.set(String.format("%s|%s|%s"
                                , imsi
                                , imei
                                , complainHost));
                        context.write(mapKey, mapVal);
                    }

                }
            }

        }
    }


}