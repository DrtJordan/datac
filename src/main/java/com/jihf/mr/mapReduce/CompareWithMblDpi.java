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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Func： 手机号md5匹配，
 * Desc:一个参数====匹配样本数据；二个参数====匹配数据源，匹配样本数据；三个参数====匹配数据源，匹配样本数据，输出目录
 * Author：JHF
 * Data：2017-08-17 08:22
 * Mail：jihaifeng@raiyi.com
 */
public class CompareWithMblDpi {
    private static final String NUM = "0";
    private static final String NUM1 = "1";
    private static final IntWritable DATA_ALL = new IntWritable(100);
    private static final IntWritable DATA_1 = new IntWritable(99);
    private static final IntWritable DATA_2 = new IntWritable(98);
    private static List<String> hostList = new ArrayList<String>();

    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class dataMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String data = value.toString();
            if (StringUtils.strIsEmpty(data)) {
                JobUtils.exit("input data is null.");
            }
            String[] datas = data.split("\\|", -1);
            if (datas.length < 39) {
                JobUtils.exit("data is empty.");
            }
            String msisdn = datas[1];
            String imei = datas[2];
            String startTime = TimeUtils.formatTime(datas[17]);
            String endTime = TimeUtils.formatTime(datas[18]);
            String duration = datas[19];
            String closeCause = StringUtils.strIsEmpty(datas[27]) ? "-1" : datas[27];
            String destinationURL = datas[29];
            String domainName = datas[30];
            String ua = datas[28];

            // 获取访问的链接里面带有换手机号的
            String phoneTemp = StringUtils.getTelnum(destinationURL);
            String phoneStr = null;
            if (!StringUtils.strIsEmpty(phoneTemp)) {
                phoneStr = phoneTemp;
            }
            if (!StringUtils.strIsEmpty(msisdn)) {
                context.write(new Text(msisdn), new Text(NUM + "|" + domainName));
            }
        }
    }

    //
    public static class dataMapper2 extends Mapper<LongWritable, Text, Text, Text> {
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

//    public static class compareCombiner extends Reducer<Text, Text, Text, Text> {
//        private MultipleOutputs output;
//
//        @Override
//        protected void setup(Context context) throws IOException, InterruptedException {
//            output = new MultipleOutputs(context);
//        }
//
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//
//            boolean flag1 = false;
//            boolean flag2 = false;
//            List<String> hostList = new ArrayList<String>();
//            List<String> textList = new ArrayList<String>();
//            String phoneMd5 = key.toString();
//            String phone = null;
//            String host = null;
//            int count = 0;
//            for (Text text : values) {
//                String[] strs = text.toString().split("\\|", -1);
//                if (strs[0].equals("0")) {
//                    host = strs[1];
//                    hostList.add(host);
//                    textList.add(text.toString());
//                    flag1 = true;
//                } else if (strs[0].equals("1")) {
//                    phone = strs[1];
//                    flag2 = true;
//                }
//                count++;
//            }
//            context.write(new Text(flag1 + "|" + flag2 + "|" + count), new Text(DATA_ALL.toString()));
//        }
//    }


    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class compareReducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs output;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            output = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            writeOut(key, values, output);
//            context.write(key, new Text());
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            output.close();
        }

    }

    public static void main(String[] args) {
        // 判空

        String input1 = Config.MOBILE_DPI_INPUT;
        String input2 = "jihaifeng/testNum/6-7月phoneNum.txt";
        String output = Config.MOBILE_DPI_OUTPUT;
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

        try {
            Job job = Job.getInstance(cf);
            job.setJobName("CompareData");

            // 通过class名称查找jar包
            job.setJarByClass(CompareWithMblDpi.class);

            // 设置输入文件按照什么格式被读取
//            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // 自定义Map的输出数据类型
            //            job.setMapperClass(dataMapper1.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);


//            job.setCombinerClass(compareCombiner.class);


            // 自定义reduce的输出数据类型
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setReducerClass(compareReducer.class);


            // MultipleInputs类添加文件路径
            MultipleInputs.addInputPath(job, new Path(input1),
                    SequenceFileInputFormat.class, dataMapper1.class);
            MultipleInputs.addInputPath(job, new Path(input2),
                    TextInputFormat.class, dataMapper2.class);

            // 设置输出文件夹
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

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
    }

    private static void writeOut(Text key, Iterable<Text> values, MultipleOutputs output) {
        try {
            boolean flag1 = false;
            boolean flag2 = false;
            List<String> hosts = new ArrayList<String>();

            String phoneMd5 = key.toString();
            String phone = null;
            for (Text text : values) {
                String[] strs = text.toString().split("\\|", -1);
                if (strs[0].equals("0")) {
                    hosts.add(strs[1]);
                    flag1 = true;
                } else if (strs[0].equals("1")) {
                    phone = strs[1];
                    flag2 = true;
                }

            }
            if (flag1 && flag2) {
                for (String ht : hosts) {
                    output.write(new Text(String.format("%s|%s|%s", phone, phoneMd5, ht)), new Text(), "result");
                }

            } else if (flag1) {
                for (String ht : hosts) {
                    output.write(new Text(String.format("%s|%s", phoneMd5, ht)), new Text(), "or1");
                }
            } else if (flag2) {
                output.write(new Text(String.format("%s|%s", phone, phoneMd5)), new Text(), "or2");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
