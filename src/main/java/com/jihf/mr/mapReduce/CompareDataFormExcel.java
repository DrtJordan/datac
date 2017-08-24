package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.excel.ExcelInputFormat;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-17 08:22
 * Mail：jihaifeng@raiyi.com
 */
public class CompareDataFormExcel {
    private static final IntWritable NUM = new IntWritable(1);
    private static final IntWritable NUM2 = new IntWritable(2);

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
            int endTimeHour = Integer.parseInt(endTime.substring(11, 13));

            // 获取访问的链接里面带有换手机号的
            String phoneTemp = StringUtils.getTelnum(destinationURL);
            String phoneStr = null;
            if (!StringUtils.strIsEmpty(phoneTemp)) {
                phoneStr = phoneTemp;
            }
            if (!StringUtils.strIsEmpty(msisdn)) {
                context.write(new Text(msisdn), new Text("0|" + phoneStr));
            }
        }
    }

    public static class dataMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                String[] datas = line.split("\\t", -1);
                String phone = datas[7];
                if (!StringUtils.strIsEmpty(phone)) {
                    String md5Str = MD5Utils.EncoderByMd5(phone);
                    context.write(new Text(md5Str), new Text("1|" + phone));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class compareReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean flag1 = false;
            boolean flag2 = false;
            String phone = null;
            for (Text text : values) {
                String[] strs = text.toString().split("\\|", -1);
                if (strs[0].equals("0")) {
                    flag1 = true;
                } else if (strs[0].equals("1")) {
                    flag2 = true;
                    phone = strs[1];
                }
            }
            if (flag1 && flag2) {
                context.write(NullWritable.get(), new Text(key + "|" + phone));
            }
        }
    }

    public static void main(String[] args) {
        // 判空

        String input1 = Config.MOBILE_DPI_INPUT;
        String input2 = "jihaifeng/testNum/testPhone.xls";
        String output = Config.MOBILE_DPI_OUTPUT;

        if (null != args) {
            if (args.length == 1) {
                input2 = args[0];
            } else if (args.length == 2) {
                input1 = args[0];
                input2 = args[1];
            } else if (args.length == 3) {
                input1 = args[0];
                input2 = args[1];
                output = args[2];
            }else {
                JobUtils.exit("the num of parameter is illegal.");
            }
        }


        Configuration cf = new Configuration();
        try {
            Job job = Job.getInstance(cf);
            job.setJobName("CompareData");

            // 通过class名称查找jar包
            job.setJarByClass(CompareDataFormExcel.class);

            // 设置输入文件按照什么格式被读取
//            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // 自定义Map的输出数据类型
            //            job.setMapperClass(dataMapper1.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 自定义reduce的输出数据类型
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setReducerClass(compareReducer.class);


            // MultipleInputs类添加文件路径
            MultipleInputs.addInputPath(job, new Path(input1),
                    SequenceFileInputFormat.class, dataMapper1.class);
            MultipleInputs.addInputPath(job, new Path(input2),
                    ExcelInputFormat.class, dataMapper2.class);
//            FileInputFormat.addInputPath(job, new Path(Config.MOBILE_DPI_INPUT));

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
}
