package com.jihf.mr.mapReduce;

import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.JobUtils;
import com.jihf.mr.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Func：投诉用户访问的域名排序
 * Desc: job取数据 ， job2按照访问数排序
 * Author：JHF
 * Data：2017-08-09 09:26
 * Mail：jihaifeng@raiyi.com
 */
public class ComplainHostSort {

    public static class myComparator extends IntWritable.Comparator {

        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    /**
     * 投诉用户样本数据   手机号|手机号MD5|域名  result  Data
     */
    public static class hostMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            if (StringUtils.strIsEmpty(data)) {
                JobUtils.exit("input data is null.");
            }

            String[] datas = data.split("\\|", -1);
            if (datas.length < 3) {
                JobUtils.exit("input data has missed some field.");
            }
            String domainName = datas[2];

            if (!StringUtils.strIsEmpty(domainName)) {
                context.write(new Text(domainName.toLowerCase()), new Text(data));
            }
        }
    }

    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class hostReducer extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            String data = null;
            String host = key.toString();
            for (Text val : values) {
                count++;
                data = val.toString();
            }
            context.write(NullWritable.get(), new Text(count + "|" + host));
        }
    }


    public static void main(String[] args) {
        // 判空
        String input1 = null;
        String output = null;
        if (null != args && args.length == 2) {
            input1 = args[0];
            output = args[1];
        } else {
            JobUtils.exit("the num of parameter is illegal.");
        }
        if (!StringUtils.strIsNotEmpty(input1, output)) {
            JobUtils.exit("the num of parameter is illegal.");
        }
        Configuration cf = new Configuration();
        try {
            Job job = Job.getInstance(cf);
            job.setJobName("HostVisitSort");

            // 通过class名称查找jar包
            job.setJarByClass(ComplainHostSort.class);

            // 设置输入文件按照什么格式被读取
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //
            job.setMapperClass(hostMapper.class);
            job.setReducerClass(hostReducer.class);

            // 自定义Map的输出数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // 自定义reduce的输出数据类型
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setSortComparatorClass(myComparator.class);

            // 设置输入文件夹
            FileInputFormat.addInputPath(job, new Path(input1));

            // 设置输出文件夹
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            if (job.waitForCompletion(true)) {
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            }
        } catch (Exception e) {
            JobUtils.exit(e.getMessage());
            e.printStackTrace();
        }


    }

}
