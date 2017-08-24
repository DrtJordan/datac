package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.excel.ExcelInputFormat;
import com.jihf.mr.utils.HDFSFileUtils;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-17 15:04
 * Mail：jihaifeng@raiyi.com
 */
public class ReadExcel {

    public static class excelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            context.write(new Text(line), new IntWritable(1));
        }
    }

    public static class excelReduce extends Reducer<Text, IntWritable, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), key);
        }
    }

    public static void main(String[] args) {
        try {

            String input1 = Config.MOBILE_DPI_INPUT + "MBLDPI4G.2017042807_141.1493334008930.lzo_deflate";
            String input2 = "jihaifeng/testNum/testPhone.xls";
            String output = Config.MOBILE_DPI_OUTPUT;


            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "readExcel");
            job.setJarByClass(ReadExcel.class);

            job.setInputFormatClass(ExcelInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(excelMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputKeyClass(IntWritable.class);

            job.setReducerClass(excelReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(input2));
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(conf, output));

            System.out.println(job.waitForCompletion(true) ? 0 : 1);


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
