package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.*;
import com.jihf.mr.utils.StringUtils;
import com.sun.codemodel.internal.JForEach;
import org.apache.commons.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.jihf.mr.utils.StringUtils.strIsEmpty;

/**
 * Func：固网语音数据匹配投诉电话
 * Desc:
 * Author：JHF
 * Data：2017-08-16 14:32
 * Mail：jihaifeng@raiyi.com
 */
public class MatchMblComplain {
    private static Matcher matcher = new Matcher(true);
    private static List<String> matchCdrList = new ArrayList<String>();
    private static List<String> matchHostList = new ArrayList<String>();

    /**
     * make the MobileCdrData as the input data one
     */
    public static class cdrMblMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapKey = new Text();
        private Text mapVal = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (matchCdrList.size() == 0) {
                matchCdrList.add("12300");
                matchCdrList.add("12321");
                matchCdrList.add("12315");
//                matchCdrList.add("10000");
//                matchCdrList.add("10001");
                 // raiyi 投诉电话
                matchCdrList.add("4008518832");
                matchCdrList.add("4006198838");
                matchCdrList.add("4008232468");

            }
            for (int i = 0; i < matchCdrList.size(); i++) {
                matcher.addPattern(MD5Utils.EncoderByMd5(matchCdrList.get(i)), i);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = getSplitData(value, 45);
            if (null != datas) {
                String callType = datas[1];
                String imsi = datas[3];
                String msisdn = datas[4];
                String other_rarty = datas[5];
                String third_rarty = datas[6];
                String callDuration = datas[9];
                String imei = datas[39];

                Matcher.MatchResult[] matchResults = null;
                if (!StringUtils.strIsEmpty(other_rarty)) {
                    matchResults = matcher.match(other_rarty);
                }
                if ((null == matchResults || matchResults.length == 0) && !StringUtils.strIsEmpty(third_rarty)) {
                    matchResults = matcher.match(third_rarty);
                }
                if (null != matchResults && matchResults.length != 0) {
                    for (Matcher.MatchResult result : matchResults) {
                        String complainNum = matchCdrList.get(Integer.parseInt(result.data.toString()));
                        if (!StringUtils.strIsEmpty(complainNum)) {
                            mapKey.set(msisdn);
                            mapVal.set(String.format("%s|%s|%s|%s|%s"
                                    , imsi
                                    , imei
                                    , complainNum
                                    , callType
                                    , callDuration));
                            context.write(mapKey, mapVal);
                        }

                    }
                }

            }
        }


    }

    /**
     * make the MobileDpiData as the input data two
     */
    public static class dpiMblMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text mapKey = new Text();
        private Text mapVal = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (matchHostList.size() == 0) {
                matchHostList.add("jbzs.12321.cn");
                matchHostList.add("c.interface.gootion.com");
                matchHostList.add("12321");
                matchHostList.add("110.360.cn");
                matchHostList.add("c.interface.at321.cn");
                matchHostList.add("data.haoma.sogou.com");
            }
            for (int i = 0; i < matchHostList.size(); i++) {
                matcher.addPattern(matchHostList.get(i), i);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = getSplitData(value, 40);
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


    /**
     * outPut the data of match
     */
    public static class cbrMblReduce extends Reducer<Text, Text, NullWritable, Text> {


        private MultipleOutputs output;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            output = new MultipleOutputs(context);
        }


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String phoneMd5 = key.toString(); // 用户手机号MD5

            List<String> complainNumList = new ArrayList<String>(); // 投诉号码列表
            String callType;
            int cdrDataCount = 0; // 投诉电话次数
            long callDurationTotal = 0; // 投诉电话总时长
            long callDurationMax = 0;// 投诉电话最大时长

            int dpiDataCount = 0; // 投诉域名次数
            String host = null; // 投诉的域名
            List<String> infoList = new ArrayList<String>();

            for (Text t : values) {
                String[] _temp = t.toString().split("\\|", -1);
                if (_temp.length == 5) {
                    cdrDataCount++;
                    String imsi = StringUtils.strIsEmpty(_temp[0]) ? null : _temp[0];
                    String imei = StringUtils.strIsEmpty(_temp[1]) ? null : _temp[1];
                    String info = String.format("%s|%s", imei, imsi);
                    if (!infoList.contains(info)) {
                        infoList.add(info);
                    }

                    String comPlainNum = _temp[2];
                    callType = getCallType(_temp[3]);

                    String complain = String.format("%s\040%s", comPlainNum, callType);
                    if (!complainNumList.contains(complain)) {
                        complainNumList.add(complain);
                    }

                    if (!StringUtils.strIsEmpty(_temp[4]) && _temp[4].matches("\\d+")) {
                        long callDurationTemp = Long.parseLong(_temp[4]);
                        callDurationTotal += callDurationTemp;
                        if (callDurationTemp > callDurationMax) {
                            callDurationMax = callDurationTemp;
                        }
                    }


                } else if (_temp.length == 3) {
                    dpiDataCount++;
                    String imsi = StringUtils.strIsEmpty(_temp[0]) ? null : _temp[0];
                    String imei = StringUtils.strIsEmpty(_temp[1]) ? null : _temp[1];
                    String info = String.format("%s|%s", imei, imsi);
                    if (!infoList.contains(info)) {
                        infoList.add(info);
                    }
                    host = _temp[2];
                }
            }

            long score = getCdrScore(cdrDataCount, host);
            if (score > 0) {
                String data = String.format("%s|%s|%s|%s|%s|%s|%s"
                        , phoneMd5
                        , complainNumList.toString()
                        , callDurationTotal
                        , callDurationMax
                        , cdrDataCount
                        , dpiDataCount
                        , score);

                if (null != data && dpiDataCount > 0) {
                    context.write(NullWritable.get(), new Text(data));
                }
            }

        }


    }

    public static void main(String[] args) {
        try {

            String input1 = Config.MOBILE_CDR_INPUT;
            String input2 = Config.MOBILE_DPI_INPUT;
            String output = Config.MOBILE_CDR_OUTPUT;
            if (null != args && args.length != 0) {
                if (args.length == 3) {
                    input1 = args[0];
                    input2 = args[1];
                    output = args[2];
                } else {
                    JobUtils.exit("the num of parameter is illegal <mblCdrFolder mblDpiFolder outputFolder>.");
                }
            }

            Configuration cf = MrUtils.getRaiyiConfiguration();
            Job job = Job.getInstance(cf, "cbrCdrJob");

            job.setJarByClass(MatchMblComplain.class);

//            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

//            job.setMapperClass(cdrMblMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(cbrMblReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


//            FileInputFormat.addInputPath(job, new Path(input));
            // MultipleInputs类添加文件路径
            MultipleInputs.addInputPath(job, new Path(input1),
                    TextInputFormat.class, cdrMblMapper.class);
            MultipleInputs.addInputPath(job, new Path(input2),
                    SequenceFileInputFormat.class, dpiMblMapper.class);

            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输入目录：" + input2);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.out.println(job.waitForCompletion(true) ? 0 : 1);
        } catch (
                Exception e)

        {
            e.printStackTrace();
        }

    }


    /**
     * 匹配投诉手机号
     *
     * @param phonemd5 源数据
     * @return 投诉的号码，没有返回null
     */
    private static String getMatcherPhone(String phonemd5) {
        String phoneNum = null;
        if (phonemd5.equals(MD5Utils.EncoderByMd5("12321"))) {
            phoneNum = "12321";
        } else if (phonemd5.equals(MD5Utils.EncoderByMd5("12300"))) {
            phoneNum = "12300";
//        } else if (phonemd5.equals(MD5Utils.EncoderByMd5("12315"))) {
//            phoneNum = "12315";
//        } else if (phonemd5.equals(MD5Utils.EncoderByMd5("10000"))) {
//            phoneNum = "10000";
//        } else if (phonemd5.equals(MD5Utils.EncoderByMd5("10001"))) {
//            phoneNum = "10001";
        }
        return phoneNum;
    }


    /**
     * 根据公式得到投诉语音的得分
     *
     * @param count 投诉电话拨打的次数
     * @param host  投诉域名
     * @return 得分
     */
    private static long getCdrScore(int count, String host) {
        int x = 30;
        int y = 20;
        // A ：用户访问网站，有访问则记为  1，无访问 则为 0；
        int A = StringUtils.strIsEmpty(host) ? 0 : 1;
        // B ：呼叫次数 （号码包括：10000、12300、12321），按次数计算
        int B = count < 0 ? 0 : count;

        return A * x + B * y;
    }

    /**
     * 匹配投诉电话类型
     *
     * @param str 投诉类型                 01：主叫   02：被叫   03：呼转
     * @return 投诉类型     未知：unKnown  打出：out   打进：int  呼转：third
     */
    private static String getCallType(String str) {
        if (strIsEmpty(str)) {
            return "unKnown";
        } else if (str.equals("01")) {
            return "out";
        } else if (str.equals("02")) {
            return "in";
        } else if (str.equals("03")) {
            return "third";
        } else {
            return "unKnown";
        }

    }

    /**
     * 输入数据转数组
     *
     * @param value     输入数据
     * @param minLength 数据最小长度
     * @return 数组数据
     */
    private static String[] getSplitData(Text value, int minLength) {
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

    /**
     * 判断是否是投诉域名
     *
     * @param str 源数据
     * @return true 是投诉域名 ， 反之则不是
     */
    private static boolean isComplainHost(String str) {
        return str.contains("jbzs.12321.cn/12321SMSReport/") || str.contains("c.interface.gootion.com/ws/v2/numbermark");
    }
}
