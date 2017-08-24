package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.JobUtils;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Func：统计移网DPI数据中用户访问网站的域名以及访问次数
 * Desc: job取数据 ， job2按照访问数排序
 * Author：JHF
 * Data：2017-08-09 09:26
 * Mail：jihaifeng@raiyi.com
 */
public class MblDpiHostSort {
    private static final IntWritable NUM = new IntWritable(1);


    public static class hostMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String data = value.toString();
            if (strIsEmpty(data)) {
                exit("input data is null.");
            }

            String[] datas = data.split("\\|", -1);
            if (datas.length < 40) {
                exit("input data has missed some field.");
            }

            String domainName = datas[30];
            String msisdn = datas[1];
            if (!strIsEmpty(msisdn) && !strIsEmpty(domainName)) {
                context.write(new Text(domainName), NUM);
            }

        }
    }


    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class hostReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count++;
            }
            if (count > 0) {
                context.write(NullWritable.get(), new Text(key + "|" + count));
            }

        }
    }

    public static class sortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\|", -1);
            context.write(new IntWritable(Integer.parseInt(split[1])), new Text(split[0]));
        }
    }

    public static class sortReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(NullWritable.get(), new Text(key + "|" + text));
            }
        }
    }

    public static void main(String[] args) {
        // 判空
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

        String outPutSort = output + "/sort";
        Configuration cf = new Configuration();
        try {
            Job job = Job.getInstance(cf);
            job.setJobName("QQVisitJob");

            // 通过class名称查找jar包
            job.setJarByClass(MblDpiHostSort.class);

            // 设置输入文件按照什么格式被读取
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //
            job.setMapperClass(hostMapper.class);
            job.setReducerClass(hostReducer.class);

            // 自定义Map的输出数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            // 自定义reduce的输出数据类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);


            // 设置输入文件夹
            FileInputFormat.addInputPath(job, new Path(input1));

            // 设置输出文件夹
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf,output));


            Job job2 = Job.getInstance(cf);

            job2.setJobName("sortJob");

            // 通过class名称查找jar包
            job2.setJarByClass(MblDpiHostSort.class);

            // 设置输入文件按照什么格式被读取
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            //
            job2.setMapperClass(sortMapper.class);
            job2.setReducerClass(sortReducer.class);

            // 自定义Map的输出数据类型
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);

            // 自定义reduce的输出数据类型
            job2.setOutputKeyClass(NullWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(output));

            FileOutputFormat.setOutputPath(job2,HDFSFileUtils.getPath(cf,outPutSort));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输出目录：" + output);
            System.out.println("排序后的目录：" + outPutSort);
            System.out.println("\n==================================\n");

            if (job.waitForCompletion(true)) {
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            }
        } catch (Exception e) {
            exit(e.getMessage());
            e.printStackTrace();
        }


    }

    // 输入文件去重
    private static boolean isContain(Job job, String str) {
        Path[] paths = FileInputFormat.getInputPaths(job);
        boolean flag = false;
        for (Path p : paths) {
            if (p.toUri().getPath().equals(new Path(str).toString())) {
                flag = true;
            }
        }
        return flag;
    }

    // 异常退出
    private static void exit(String msg) {
        System.out.println(strIsEmpty(msg) ? "抛出异常" : msg);
        System.exit(0);
    }

    private static boolean strIsEmpty(String str) {
        return null == str || str.trim().length() <= 0;
    }
}
