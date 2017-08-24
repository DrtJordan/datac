package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.JobUtils;
import com.jihf.mr.utils.MD5Utils;
import com.jihf.mr.utils.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Func：固网手机号
 * Desc:
 * Author：JHF
 * Data：2017-08-11 14:12
 * Mail：jihaifeng@raiyi.com
 */
public class FixDpiPhoneNum {

    public static class FixMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fixData = value.toString();
            String imeiStr = null;
            String imsiStr = null;
            String phoneStr = null;

            // 取imei
            if (fixData.contains("imei=")) {
                int index = fixData.indexOf("imei=");
                String tempData = fixData.substring(index, fixData.length());
                if (!StringUtils.strIsEmpty(tempData) && tempData.length() > 5) {
                    int from = tempData.indexOf("=");
                    int to = tempData.length();
                    if (tempData.contains("&")) {
                        to = tempData.indexOf("&");
                    }

                    String imeiTemp = tempData.substring(from + 1, to);
                    if (!StringUtils.strIsEmpty(imeiTemp) && !imeiTemp.equals("000000000000000")) {
                        int to1 = imeiTemp.length();
                        if (imeiTemp.contains(";")) {
                            to1 = imeiTemp.indexOf(";");
                        }
                        String imeiTemp1 = imeiTemp.substring(0, to1);
                        if (StringUtils.isNumericZidai(imeiTemp1) && imeiTemp1.length() == 15) {
                            imeiStr = imeiTemp1;
                        }
                    }

                }

            }

            // 取imsi
            if (fixData.contains("imsi=")) {
                int index = fixData.indexOf("imsi=");
                String tempData = fixData.substring(index, fixData.length());
                if (!StringUtils.strIsEmpty(tempData) && tempData.length() > 5) {
                    int from = tempData.indexOf("=");
                    int to = tempData.length();
                    if (tempData.contains("&")) {
                        to = tempData.indexOf("&");
                    }

                    String imsiTemp = tempData.substring(from + 1, to);
                    String imsiTemp1 = null;
                    if (imsiTemp.length() > 15) {
                        imsiTemp1 = imsiTemp.substring(0, 16);

                    } else {
                        imsiTemp1 = imsiTemp;
                    }
                    if (!StringUtils.strIsEmpty(imsiTemp1) && !imsiTemp1.equals("000000000000000") && imsiTemp1.length() == 15) {
                        imsiStr = imsiTemp1;
                    }

                }
            }

            String domainName = StringUtils.getDomainName(fixData);


            // 取手机号
            String phoneTemp = StringUtils.getTelnum(fixData);
            if (!StringUtils.strIsEmpty(phoneTemp)) {
                phoneStr = phoneTemp;
            }
            if (!StringUtils.strIsEmpty(imeiStr)) {
                String msisdn = null;
                if (!StringUtils.strIsEmpty(phoneStr)) {
                    msisdn = MD5Utils.EncoderByMd5(phoneStr);
                }
                if (!StringUtils.strIsEmpty(msisdn)) {
                    context.write(new Text(phoneStr), new IntWritable(1));
                }
//                context.write(new Text("msisdn：" + msisdn
//                        + "|imei：" + imeiStr
//                        + "|imsi：" + imsiStr
//                        + "|phone：" + phoneStr
//                        + "|domainName：" + domainName), new IntWritable(1));
            }
        }


    }


    public static class FixReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable i : values) {
                count++;
            }
            context.write(new Text(key), NullWritable.get());
        }

    }

    public static void main(String[] args) {

        try {

            String input1 = Config.FIX_DPI_INPUT;
            String output = Config.FIX_DPI_OUTPUT;
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

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "fixData");
            job.setJarByClass(FixDpiPhoneNum.class);

            job.setMapperClass(FixMapper.class);
            job.setReducerClass(FixReducer.class);

            job.setMapOutputValueClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);


            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(input1));
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(conf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录："+input1);
            System.out.println("输出目录："+output);
            System.out.println("\n==================================\n");

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            JobUtils.exit(e.getMessage());
            e.printStackTrace();
        }

    }
}
