package com.jihf.mr.mapReduce;

import com.jihf.mr.utils.JobUtils;
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

import java.io.IOException;

/**
 * Func：从测试集群copy数据到本机
 * Desc:
 * Author：JHF
 * Data：2017-08-11 09:10
 * Mail：jihaifeng@raiyi.com
 */
public class CopyFile {

    private static final IntWritable NUM = new IntWritable(1);


    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class copyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String data = value.toString();
            if (StringUtils.strIsEmpty(data)) {
                JobUtils.exit("input data is null.");
            }
            String[] datas = data.split("\\|", -1);

            context.write(new Text("msisdn：" + datas[1] + "|imsi：" + datas[0] + "|imei：" + datas[2]), NUM);
        }
    }


    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class copyReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            context.write(NullWritable.get(), key);

        }
    }

    public static void main(String[] args) {

        Configuration cf = new Configuration();
        try {
            Job job = Job.getInstance(cf);
            job.setJobName("QQVisitJob");

            // 通过class名称查找jar包
            job.setJarByClass(CopyFile.class);

            // 设置输入文件按照什么格式被读取
            job.setInputFormatClass(SequenceFileInputFormat.class);

            //
            job.setMapperClass(copyMapper.class);
            job.setReducerClass(copyReducer.class);

            // 自定义Map的输出数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            // 自定义reduce的输出数据类型
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            // 设置输入文件夹
            FileInputFormat.setInputPaths(job, new Path(args[0]));

            System.out.println("===================================");


            System.out.println("===================================");

            // 设置输出文件夹
            if (StringUtils.strIsEmpty(args[args.length - 1])) {
                JobUtils.exit("output folder is illegal.");
            }
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            JobUtils.exit(e.getMessage());
            e.printStackTrace();
        }
    }

}
