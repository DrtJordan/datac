package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Func：投诉用户中拨打投诉电话的
 * Desc:
 * Author：JHF
 * Data：2017-08-16 14:32
 * Mail：jihaifeng@raiyi.com
 */
public class MatchMblCdrComplain {
    private static final String NUM = "0";
    private static final String NUM1 = "1";

    public static class cdrMblMapper extends Mapper<LongWritable, Text, Text, Text> {
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
                String phoneComplain = null;
                String phone1 = null;
                String phone2 = null;
                if (!StringUtils.strIsEmpty(other_rarty)) {
                    phone1 = getMatcherPhone(other_rarty);
                }
                if (!StringUtils.strIsEmpty(third_rarty)) {
                    phone2 = getMatcherPhone(third_rarty);
                }
                if (!StringUtils.strIsEmpty(phone1)) {
                    phoneComplain = phone1;
                } else if (!StringUtils.strIsEmpty(phone2)) {
                    phoneComplain = phone2;
                }

                if (!StringUtils.strIsEmpty(phoneComplain)) {

                    context.write(new Text(msisdn), new Text(NUM
                            + "|" + phoneComplain
                            + "|" + callType
                            + "|" + callDuration));
                }

            }

        }

        private String getMatcherPhone(String phonemd5) {
            String phoneNum = null;
            if (phonemd5.equals(MD5Utils.EncoderByMd5("12321"))) {
                phoneNum = "12321";
            } else if (phonemd5.equals(MD5Utils.EncoderByMd5("12300"))) {
                phoneNum = "12300";
            } else if (phonemd5.equals(MD5Utils.EncoderByMd5("12315"))) {
                phoneNum = "12315";
            } else if (phonemd5.equals(MD5Utils.EncoderByMd5("10000"))) {
                phoneNum = "10000";
            } else if (phonemd5.equals(MD5Utils.EncoderByMd5("10001"))) {
                phoneNum = "10001";
            }
            return phoneNum;
        }
    }

    public static class cdrMblMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String md5Str = line;
            if (line.length() <= 11) {
                md5Str = MD5Utils.EncoderByMd5(line);
            }
            context.write(new Text(md5Str), new Text(NUM1 + "|" + line));

        }
    }

    public static class cbrMblReduce extends Reducer<Text, Text, NullWritable, Text> {


        private MultipleOutputs output;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            output = new MultipleOutputs(context);
        }


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean flag1 = false;
            boolean flag2 = false;

            int count = 0;
            String phoneMd5 = key.toString(); // 用户手机号MD5

            String phone = null; // 用户手机号
//            String phoneComplain = null; // 投诉号码
//            String callType = "unKnown"; // 呼叫类型
            List<String> phoneComplain = new ArrayList<String>(); // 投诉号码
            long callDuration = 0; // 呼叫时长
            long callDurationMax = 0;
            long callD = 0;

            for (Text text : values) {
                String[] strs = text.toString().split("\\|", -1);
                if (strs[0].equals(NUM)) {
                    flag1 = true;

                    String complain =String.format("%s\040%s", strs[1], getCallType(strs[2]));
                    if (!phoneComplain.contains(complain)) {
                        phoneComplain.add(complain);
                    }

                    if (!StringUtils.strIsEmpty(strs[3]) || strs[3].matches("\\d+")) {
                        callD = Long.parseLong(strs[3]);
                    }
                    callDuration += callD;
                    if (callD > callDurationMax) {
                        callDurationMax = callD;
                    }
                    count++;
                } else if (strs[0].equals(NUM1)) {
                    flag2 = true;
                    phone = strs[1];
                }

            }


            if (flag1 && flag2) {
                long score = getScore(count, callDuration);
                String data = String.format("%s|%s|%s|%s|%s|%s",
                        phone,
                        phoneComplain,
                        callDuration,
                        callDurationMax,
                        count,
                        score);
                output.write(NullWritable.get(), new Text(data), "result");
            } else if (flag2) {
                String data = String.format("%s|%s", phone, phoneMd5);
                output.write(NullWritable.get(), new Text(data), "original");
            } else if (flag1) {
                String data = String.format("%s|%s|%s|%s|%s",
                        phone,
                        phoneComplain,
                        callDuration,
                        callDurationMax,
                        count);
                output.write(NullWritable.get(), new Text(data), "original2");
            }

        }

        private long getScore(int count, long callDuration) {
            long score1 = 0;
            long score2 = 0;
            int y = 20;
            int z = 10;
            int B = count;
            long C = callDuration;
            score1 = (B - 3) * y < 0 ? 0 : (B - 3) * y;
            score2 = (C - 120) / 10 * z < 0 ? 0 : (C - 120) / 10 * z;

            return score1 + score2;
        }

        private String getCallType(String str) {
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
    }

    public static void main(String[] args) {
        try {

            String input1 = Config.MOBILE_CDR_INPUT;
            String input2 = "jihaifeng/testNum/6-7月phoneNum.txt";
            String output = Config.MOBILE_CDR_OUTPUT;
            if (null != args && args.length != 0) {
                if (args.length == 1) {
                    input2 = args[0];
                } else if (args.length == 2) {
                    input1 = args[0];
                    input2 = args[1];
                } else if (args.length == 3) {
                    input1 = args[0];
                    input2 = args[1];
                    output = args[2];
                } else {
                    JobUtils.exit("the num of parameter is illegal.");
                }
            }

            Configuration cf = MrUtils.getRaiyiConfiguration();
            Job job = Job.getInstance(cf, "CdrJob");
            job.setJarByClass(MatchMblCdrComplain.class);

            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(cbrMblReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


            // MultipleInputs类添加文件路径
            MultipleInputs.addInputPath(job, new Path(input1),
                    TextInputFormat.class, cdrMblMapper.class);
            MultipleInputs.addInputPath(job, new Path(input2),
                    TextInputFormat.class, cdrMblMapper2.class);

            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输入目录：" + input2);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.out.println(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
