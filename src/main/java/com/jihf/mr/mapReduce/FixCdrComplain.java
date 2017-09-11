package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Func：固网语音数据匹配投诉电话
 * Desc:
 * Author：JHF
 * Data：2017-08-16 14:32
 * Mail：jihaifeng@raiyi.com
 */
public class FixCdrComplain {
    public static final IntWritable NUM = new IntWritable(1);

    public static class cbrFixMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] datas = data.split("\\|", -1);
            if (datas.length > 25) {
                String callType = datas[1];
                String msisdn = datas[2];
                String other_rarty = datas[3];
                String third_rarty = datas[4];
                String sTime = datas[9];
                String eTime = datas[10];
                String callDuration = datas[11];
                String phone = null;
                String phone1 = null;
                String phone2 = null;
                if (!StringUtils.strIsEmpty(other_rarty)) {
                    phone1 = getMatcherPhone(other_rarty);
                }
                if (!StringUtils.strIsEmpty(third_rarty)) {
                    phone2 = getMatcherPhone(third_rarty);
                }
                if (!StringUtils.strIsEmpty(phone1)) {
                    phone = phone1;
                } else if (!StringUtils.strIsEmpty(phone2)) {
                    phone = phone2;
                }

                if (!StringUtils.strIsEmpty(phone) && (callType.equals("01") || callType.equals("02"))) {
                    context.write(new Text(phone), new Text(callType
                            + "|" + msisdn
                            + "|" + callDuration));
                }

            }

        }


    }

    public static class cbrFixReduce extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(NullWritable.get(), new Text(key + "\t" + text));
            }

        }
    }

    public static void main(String[] args) {
        try {

            String input1 = Config.FIX_CDR_INPUT;
            String output = Config.FIX_CDR_OUTPUT;
            if (null != args && args.length != 0) {
                if (args.length == 1) {
                    input1 = args[0];
                } else if (args.length == 2) {
                    input1 = args[0];
                    output = args[1];
                } else {
                    JobUtils.exit("the num of parameter is illegal.");
                }
            }

            Configuration cf = MrUtils.getRaiyiConfiguration();
            Job job = Job.getInstance(cf, "cbrFixJob");
            job.setJarByClass(FixCdrComplain.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(cbrFixMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(cbrFixReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path(input1));

            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.out.println(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getMatcherPhone(String phonemd5) {
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
        } else if (phonemd5.equals("D856EE5E0FF85F41076C33FC65564CE1")) {
            // test
            phoneNum = "测试数据000";
        }
        return phoneNum;
    }
}
