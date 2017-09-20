package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-19 15:17
 * Mail：jihaifeng@raiyi.com
 */
public class MatchMblDpiUAWithComplain extends Configured implements Tool {
    public static final String TAG_PHONE = "1";
    public static final String TAG_DPI = "2";

    @Override
    public int run(String[] strings) throws Exception {
        String input1 = "jihaifeng/testNum/tousu-8yue.txt";
        String input2 = Config.MOBILE_DPI_INPUT;
        String output = Config.MOBILE_DPI_OUTPUT;

        Configuration conf = MrUtils.getRaiyiConfiguration();

        Job job = Job.getInstance(conf, "GetUA");

        if (null != strings && strings.length != 0) {
            if (strings.length == 1) {
                input1 = StringUtils.strIsEmpty(strings[0]) ? null : strings[0];
            } else if (strings.length == 2) {
                input1 = StringUtils.strIsEmpty(strings[0]) ? null : strings[0];
                input2 = StringUtils.strIsEmpty(strings[1]) ? null : strings[1];
            } else if (strings.length == 3) {
                input1 = StringUtils.strIsEmpty(strings[0]) ? null : strings[0];
                input2 = StringUtils.strIsEmpty(strings[1]) ? null : strings[1];
                output = StringUtils.strIsEmpty(strings[2]) ? null : strings[2];
            } else {
                JobUtils.exit("params is illegal.please input the params like <comPlainPhone  DpiInput resultOutput>");
            }
        }
        if (StringUtils.strIsEmpty(input1, input2, output)) {
            JobUtils.exit("params is illegal.please input the params like <comPlainPhone  DpiInput resultOutput>");
        }

        job.setJarByClass(MatchMblDpiUAWithComplain.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(uaReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, phoneMapper.class);
        MultipleInputs.addInputPath(job, new Path(input2), SequenceFileInputFormat.class, dpiMapper.class);

        FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(conf, output));

        System.out.println("\n==================================\n");
        System.out.println("输入目录：" + input1);
        System.out.println("输入目录：" + input2);
        System.out.println("输出目录：" + output);
        System.out.println("\n==================================\n");

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }

    public static class phoneMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String phoneNum = StringUtils.strIsEmpty(value.toString()) ? null : value.toString();
            if (null != phoneNum) {
                context.write(new Text(MD5Utils.EncoderByMd5(phoneNum)), new Text(String.format("%s|%s", TAG_PHONE, phoneNum)));
            }
        }
    }

    public static class dpiMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\\|", -1);
            if (datas.length > 35) {
                String phoneMd5 = StringUtils.strIsEmpty(datas[1]) ? null : datas[1];
                String UA = StringUtils.strIsEmpty(datas[28]) ? null : datas[28];
                if (null != phoneMd5 && null != UA) {
                    context.write(new Text(phoneMd5), new Text(String.format("%s|%s", TAG_DPI, UA)));
                }
            }
        }
    }

    public static class uaReduce extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean flag1 = false;
            boolean flag2 = false;
            String phoneNum = null;
            List<String> uaList = new ArrayList<String>();
            for (Text val : values) {
                String[] vals = val.toString().split("\\|", -1);
                if (vals[0].equals(TAG_PHONE)) {
                    flag1 = true;
                    phoneNum = vals[1];
                } else if (vals[0].equals(TAG_DPI)) {
                    flag2 = true;
                    if (!uaList.contains(vals[1])) {
                        uaList.add(vals[1]);
                    }
                }
            }
            if (flag1 && flag2 && StringUtils.strIsNotEmpty(phoneNum) && uaList.size() > 0) {
                for (String ua : uaList) {
                    context.write(NullWritable.get(), new Text(String.format("%s|%s", phoneNum, ua)));
                }

            }
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MatchMblDpiUAWithComplain(), args);
        System.exit(exitCode);
    }
}
