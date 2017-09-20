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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

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

            String[] datas = fixData.split("\005", -1);
            if (datas.length >= 11) {
                String UserAccount = StringUtils.strIsEmpty(datas[0]) ? null : datas[0];
                String Protocol = StringUtils.strIsEmpty(datas[1]) ? null : datas[1];
                String SourceIP = StringUtils.strIsEmpty(datas[2]) ? null : datas[2];
                String DestinationIP = StringUtils.strIsEmpty(datas[3]) ? null : datas[3];
                String SourcePort = StringUtils.strIsEmpty(datas[4]) ? null : datas[4];
                String DestinationPort = StringUtils.strIsEmpty(datas[5]) ? null : datas[5];
                String DomainName = StringUtils.strIsEmpty(datas[6]) ? null : datas[6];
                String URL = StringUtils.strIsEmpty(datas[7]) ? null : datas[7];
                String REFERER = StringUtils.strIsEmpty(datas[8]) ? null : datas[8];
                String UserAgent = StringUtils.strIsEmpty(datas[9]) ? null : datas[9];
                String Cookie = StringUtils.strIsEmpty(datas[10]) ? null : datas[10];
//            String accessTime = StringUtils.strIsEmpty(datas[11]) ? null : datas[11];

                context.write(new Text(String.format("%s|%s", URL, UserAgent)), new IntWritable(1));
            }
        }
    }

    public static class FixCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 取手机号
            String[] datas = key.toString().split("\\|", -1);
            String url = datas[0];
            String ua = datas[1];

            String phonekey = null;
            String phoneStr = null;
            if (!StringUtils.strIsEmpty(url)) {
                String host = UrlHandler.getHost(url);

                Map<String, String> paramsMap = UrlHandler.URLRequest(url);

                if (null != paramsMap && paramsMap.size() != 0) {
                    for (String mapKey : paramsMap.keySet()) {
                        if (StringUtils.strIsNotEmpty(mapKey, UrlHandler.matchMblNumKey(mapKey))) {
                            phonekey = UrlHandler.matchMblNumKey(mapKey);
                        }
                    }

                    if (!StringUtils.strIsEmpty(phonekey)) {
                        phoneStr = StringUtils.getTelnum(paramsMap.get(phonekey));
                    }

                    if (!StringUtils.strIsEmpty(phoneStr)) {
                        context.write(new Text(String.format("%s|%s", phoneStr, ua)), new IntWritable(1));
                    }
                }
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

            Configuration conf = MrUtils.getRaiyiConfiguration();

            Job job = Job.getInstance(conf, "fixData");
            job.setJarByClass(FixDpiPhoneNum.class);

            job.setMapperClass(FixMapper.class);
            job.setCombinerClass(FixCombiner.class);
            job.setReducerClass(FixReducer.class);
            job.setNumReduceTasks(5);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);


            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(input1));
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(conf, output));

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

}
