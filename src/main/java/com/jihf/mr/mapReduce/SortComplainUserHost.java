package com.jihf.mr.mapReduce;

import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.MrUtils;
import com.jihf.mr.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-28 09:10
 * Mail：jihaifeng@raiyi.com
 */
public class SortComplainUserHost extends Configured implements Tool {

    @Override
    public int run(String[] strings) {
        try {
            String input = "jihaifeng/mblDpi/result-r-00000";
            String output = "jihaifeng/userHost";
            Configuration conf = MrUtils.getRaiyiConfiguration();
            Job job = Job.getInstance(conf, "userHost");

            job.setJarByClass(SortComplainUserHost.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(hostMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setReducerClass(hostReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(conf, output));


            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");


            return job.waitForCompletion(true) ? 0 : 1;

        } catch (Exception e) {
            System.err.println(e);
            return 1;
        }
    }

    public static class hostMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\\|", -1);
            String hostStr = datas[1];
            String hostTemp = hostStr.substring(1, hostStr.length() - 2);
            String[] splits = hostTemp.split(", ", -1);
            for (String split : splits) {
                if (!StringUtils.strIsEmpty(split)
                        && !split.matches("\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}")
                        && !split.contains("qq.com"))
                    context.write(new Text(split), new IntWritable(1));
            }
        }
    }

    public static class hostReduce extends Reducer<Text, IntWritable, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable i : values) {
                count++;
            }
            context.write(NullWritable.get(), new Text(String.format("%s|%s", count, key.toString())));
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SortComplainUserHost(), args);
        System.exit(exitCode);
    }

}
