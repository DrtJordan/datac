package com.jihf.mr.mapReduce.zaojiao;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Func：固网手机号
 * Desc:
 * Author：JHF
 * Data：2017-08-11 14:12
 * Mail：jihaifeng@raiyi.com
 */
public class MatchDpiHostWithZaoJiao extends Configured implements Tool {

    private static List<String> phoneList = new ArrayList<String>();
    private static Matcher matcher = new Matcher(true);

    @Override
    public int run(String[] args) throws Exception {
        try {
            String input1 = Config.MOBILE_DPI_INPUT;
            String input2 = Config.MOBILE_CDR_INPUT;
            String output = Config.MOBILE_DPI_OUTPUT;
            if (null != args && args.length != 0) {
                if (args.length == 3) {
                    input1 = args[0];
                    input2 = args[1];
                    output = args[2];
                } else {
                    JobUtils.exit("the num of parameter is illegal.");
                }
            }
            Configuration conf = MrUtils.getRaiyiConfiguration();

            Job job = Job.getInstance(conf, "fixData");
            job.setJarByClass(MatchDpiHostWithZaoJiao.class);

            job.setCombinerClass(zaojiaoCombiner.class);
            job.setReducerClass(zaojiaoReducer.class);

            job.setMapOutputValueClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);


            job.setOutputFormatClass(TextOutputFormat.class);

            // MultipleInputs类添加文件路径
            MultipleInputs.addInputPath(job, new Path(input1),
                    SequenceFileInputFormat.class, mblDPiMapper.class);
            MultipleInputs.addInputPath(job, new Path(input2),
                    TextInputFormat.class, mblCdrMapper.class);
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(conf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输入目录：" + input2);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            JobUtils.exit(e.getMessage());
            e.printStackTrace();
        }
        return 0;
    }

    public static class mblDPiMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fixData = value.toString();

            String[] datas = fixData.split("\\|", -1);
            if (datas.length > 38) {
                String imsi = StringUtils.strIsEmpty(datas[0]) ? null : datas[0];
                String msisdn = StringUtils.strIsEmpty(datas[1]) ? null : datas[1];
                String imei = StringUtils.strIsEmpty(datas[2]) ? null : datas[2];

                String UserAgent = StringUtils.strIsEmpty(datas[28]) ? null : datas[28];
                String URL = StringUtils.strIsEmpty(datas[29]) ? null : datas[29];
                String doMain = StringUtils.strIsEmpty(datas[30]) ? null : datas[30];
                String host = StringUtils.strIsEmpty(datas[31]) ? null : datas[31];
                String refer = StringUtils.strIsEmpty(datas[35]) ? null : datas[35];
                if (!StringUtils.strIsEmpty(msisdn)
                        && (!StringUtils.strIsEmpty(URL)
                        || !StringUtils.strIsEmpty(refer))) {
                    context.write(new Text(msisdn), new Text(String.format("%s|%s|%s",
                            URL,
                            refer,
                            UserAgent)));
                }

            }

        }


    }

    public static class mblCdrMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] datas = data.split("\\|", -1);
            if (datas.length > 25) {
                String callType = datas[1];
                String msisdn = datas[4];
                String other_rarty = datas[5];
                String third_rarty = datas[6];
                String callDuration = datas[9];

                if (!StringUtils.strIsEmpty(msisdn)) {

                    context.write(new Text(msisdn), new Text(String.format("%s|%s|%s|%s",
                            other_rarty,
                            third_rarty,
                            callType,
                            callDuration)));
                }

            }
        }
    }

    public static class zaojiaoCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            initMatcher();
            initNum();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 取手机号

            String url = null;
            String ua = null;
            String zaojiaoPhone = null;
            String callType = null;
            String callDuration = null;
            for (Text val : values) {
                String[] datas = val.toString().split("\\|", -1);
                // dpi数据  输出 url|ua
                if (datas.length == 3) {
                    if (matchHost(datas[0])) {
                        url = datas[0];
                    }
                    if (matchHost(datas[1])) {
                        url = datas[1];
                    }
                    ua = datas[2];
                    if (!StringUtils.strIsEmpty(key.toString()) && !StringUtils.strIsEmpty(url)) {
                        context.write(key,
                                new Text(String.format("%s|%s",
                                        url,
                                        ua)));
                    }
                }
                // cdr数据 输出  phone|callType|callDur
                if (datas.length == 4) {
                    if (null != getMatchPhone(datas[0])) {
                        zaojiaoPhone = getMatchPhone(datas[0]);
                    } else if (null != getMatchPhone(datas[1])) {
                        zaojiaoPhone = getMatchPhone(datas[1]);
                    }
                    callType = datas[2];
                    callDuration = datas[3];
                    if (!StringUtils.strIsEmpty(key.toString()) && !StringUtils.strIsEmpty(zaojiaoPhone)) {
                        context.write(key,
                                new Text(String.format("%s|%s|%s",
                                        zaojiaoPhone,
                                        callType,
                                        callDuration)));
                    }
                }

            }
        }


    }

    public static class zaojiaoReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            List<String> urlList = new ArrayList<String>();
            List<String> uaList = new ArrayList<String>();
            List<String> phoneNumList = new ArrayList<String>();
            for (Text val : values) {
                count++;
                String[] datas = val.toString().split("\\|", -1);
                if (datas.length == 2) {
                    if (!urlList.contains(datas[0])) {
                        urlList.add(datas[0]);
                        uaList.add(datas[1]);
                    }


                }
                if (datas.length == 3) {
                    phoneNumList.add(datas[0] + "-" + getCallType(datas[1]) + "-" + datas[2]);
                }
            }

            if (!StringUtils.strIsEmpty(key.toString())
                    && (urlList.size() != 0
                    || phoneNumList.size() != 0)) {
                context.write(new Text(String.format("%s|%s|%s",
                        key.toString(),
                        urlList.toString(),
                        phoneNumList.toString())), NullWritable.get());
            }
        }

    }

    private static void initMatcher() {
        matcher.addPattern("*.gymboglobal.com.cn*", 100);
        matcher.addPattern("*.bbunion.com*", 200);
        matcher.addPattern("*.combaby.cn*", 300);
        matcher.addPattern("*.rompy.cn*", 400);
        matcher.addPattern("*.gymbaby.cn*", 500);
        matcher.addPattern("*.vinciedu.com*", 600);
//        matcher.addPattern("*.guazi.com/*/sell*", 700);
    }

    private static void initNum() {
        if (null == phoneList) {
            phoneList = new ArrayList<String>();
        }
        if (phoneList.size() == 0) {
            // 金宝贝
            phoneList.add("4007009090");
            // Bbunion
            phoneList.add("4000078777");
            // 美吉姆
            phoneList.add("4006027766");
            // 红黄蓝
            phoneList.add("4008186669");
            phoneList.add("4006160162");
            phoneList.add("4000859988");
            phoneList.add("4009025988");
            // 东方爱婴
            phoneList.add("4008108858");
            // 新爱婴
            phoneList.add("4006889577");
            phoneList.add("4000192226");
            // 悦宝园
            phoneList.add("4009608188");
            // 运动宝贝
            phoneList.add("4000088593");
            phoneList.add("4007009293");
            // 亲亲袋鼠
            phoneList.add("4006551580");
            // 测试
//            phoneList.add("12315");
        }
    }

    private static boolean matchHost(String url) {
        if (StringUtils.strIsEmpty(url)) {
            return false;
        }
        if (!StringUtils.strIsEmpty(url)) {
            Matcher.MatchResult[] a = matcher.match(url);
            if (a.length != 0) {
                return true;
            }
        }
        return false;
    }

    private static String getMatchPhone(String phonemd5) {
        String phoneNum = null;
        if (StringUtils.strIsEmpty(phonemd5)) {
            return phoneNum;
        }
        if (null != phoneList && phoneList.size() > 0) {
            for (String key : phoneList) {
                if (phonemd5.equals(MD5Utils.EncoderByMd5(key))) {
                    phoneNum = key;
                }
            }
        }
        return phoneNum;
    }

    private static String getCallType(String str) {
        // 01：主叫，02：被叫，03：呼转
        if (StringUtils.strIsEmpty(str)) {
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

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MatchDpiHostWithZaoJiao(), args);
        System.exit(exitCode);
    }
}
