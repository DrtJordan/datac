package com.jihf.mr.mapReduce;

import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.MrUtils;
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
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-28 11:25
 * Mail：jihaifeng@raiyi.com
 */
public class SortData {

    public static class sortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\|", -1);
            if (!split[1].contains("qq.com") && !split[1].contains("qpic.cn")) {
                context.write(new IntWritable(Integer.parseInt(split[0])), new Text(split[1]));
            }
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
        try {
            String input1 = "jihaifeng/userHost/part-r-00000";
            String output = "jihaifeng/sortData";
            Configuration conf = MrUtils.getRaiyiConfiguration();
            Job job = Job.getInstance(conf);

            job.setJobName("sortJob");

            // 通过class名称查找jar包
            job.setJarByClass(SortData.class);

            // 设置输入文件按照什么格式被读取
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            //
            job.setMapperClass(sortMapper.class);
            job.setReducerClass(sortReducer.class);

            // 自定义Map的输出数据类型
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            // 自定义reduce的输出数据类型
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setSortComparatorClass(MblDpiHostSort.myComparator.class);

            FileInputFormat.addInputPath(job, new Path(input1));

            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(conf,output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
