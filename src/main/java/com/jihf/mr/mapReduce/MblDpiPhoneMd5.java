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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Func：移网DPI手机号MD5
 * Desc:
 * Author：JHF
 * Data：2017-08-10 11:48
 * Mail：jihaifeng@raiyi.com
 */
public class MblDpiPhoneMd5 {


    private static Matcher matcher = new Matcher(true);

    private static final IntWritable NUM = new IntWritable(1);


    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class phoneMd5Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
//            if (Integer.parseInt(closeCause) == 1) {
//            context.write(new Text("sTime：" + startTime + "|eTime：" + endTime + "|endTimeHour：" + endTimeHour + "|closeCause：" + closeCause), NUM);
//            }

            // 获取访问的链接里面带有换手机号的
            String phoneTemp = StringUtils.getTelnum(destinationURL);
            String phoneStr = null;
            if (!StringUtils.strIsEmpty(phoneTemp)) {
                phoneStr = phoneTemp;
            }
//            if (!StringUtils.strIsEmpty(phoneStr)) {
//                context.write(new Text(phoneStr), NUM);
//            }
            if (!StringUtils.strIsEmpty(msisdn) && matcherDomain(domainName)) {
                context.write(new Text(msisdn), NUM);
//                context.write(new Text(msisdn + "|" + endTime + "|" + domainName + "|" + ua), NUM);
            }


        }


    }


    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class phoneMd5Reducer extends Reducer<Text, IntWritable, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable i : values) {
                count++;
            }
            context.write(NullWritable.get(), new Text(key + ""));
//            context.write(NullWritable.get(), new Text(key + ",count：" + count));

        }
    }

    public static void main(String[] args) {
        // 判空


//        String input = "/daas/bstl/dpiqixin/jiangsu_4G/20170428/";
//        String output = "jihaifeng/mobileData";

        try {
            String input1 = Config.MOBILE_DPI_INPUT;
            String output = Config.MOBILE_DPI_OUTPUT;
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

            Configuration cf = new Configuration();

            Job job = Job.getInstance(cf);
            job.setJobName("FilterData");

            // 通过class名称查找jar包
            job.setJarByClass(MblDpiPhoneMd5.class);

            // 设置输入文件按照什么格式被读取
            job.setInputFormatClass(SequenceFileInputFormat.class);

            //
            job.setMapperClass(phoneMd5Mapper.class);
            job.setReducerClass(phoneMd5Reducer.class);

            // 自定义Map的输出数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            // 自定义reduce的输出数据类型
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            // 设置输入文件夹
            FileInputFormat.addInputPath(job, new Path(input1));

            // 设置输出文件夹
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            JobUtils.exit(e.getMessage());
            e.printStackTrace();
        }
    }

    public static boolean matcherDomain(String domainName) {
        boolean phoneNum = false;
        return true;
    }
}
