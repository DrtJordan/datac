package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.mapReduce.Mapper.MblDpiMapperForHost;
import com.jihf.mr.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Func：dpi用户中投诉域名访问
 * Desc:
 * Author：JHF
 * Data：2017-08-16 14:32
 * Mail：jihaifeng@raiyi.com
 */
public class MatchMblHostComplain {
    private static Matcher matcher = new Matcher(true);
    private static List<String> matchHostList = new ArrayList<String>();

    public static class mblDpiHostMap extends Mapper<LongWritable, Text, Text, Text> {

        private Text mapKey = new Text();
        private Text mapVal = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (matchHostList.size() == 0) {
                matchHostList.add("jbzs.12321.cn");
                matchHostList.add("c.interface.gootion.com");
                matchHostList.add("12321");
                matchHostList.add("110.360.cn");
                matchHostList.add("c.interface.at321.cn");
                matchHostList.add("data.haoma.sogou.com");

            }
            for (int i = 0; i < matchHostList.size(); i++) {
                matcher.addPattern(matchHostList.get(i), i);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = DpiDataUtils.getSplitData(value, 40);
            if (null != datas) {
                String imsi = datas[0];
                String msisdn = datas[1];
                String imei = datas[2];
                String destination = datas[29];
                String domainName = datas[30];
                String host = datas[31];
                String hostComplain = null;

                Matcher.MatchResult[] matchResults = null;


                if (!StringUtils.strIsEmpty(destination)) {
                    // 获取 destination带path值 ,去掉参数
//                    destination = destination.toUpperCase();
                    int index = destination.indexOf("HTTP://");
                    if (index != -1) {
                        destination = destination.substring(index + 7);
                    }
                    index = destination.indexOf("?");
                    if (index != -1) {
                        destination = destination.substring(0, index);
                    }
                    matchResults = matcher.match(destination);
                }

                if ((null == matchResults || matchResults.length == 0) && !StringUtils.strIsEmpty(domainName)) {
                    matchResults = matcher.match(domainName);
                }

                if ((null == matchResults || matchResults.length == 0) && !StringUtils.strIsEmpty(host)) {
                    matchResults = matcher.match(host);
                }
                if (null != matchResults && matchResults.length != 0) {
                    for (Matcher.MatchResult result : matchResults) {
                        String complainHost = result.pattern;
                        if (!StringUtils.strIsEmpty(complainHost)) {
                            mapKey.set(complainHost);
                            mapVal.set(msisdn);
                            context.write(mapKey, mapVal);
                        }

                    }
                }

            }
        }
    }

    public static class dpiHostReduce extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            String complainHost = key.toString(); // 投诉域名

            for (Text text : values) {
                count++;
            }
            context.write(NullWritable.get(), new Text(String.format("%s|%s", complainHost, count)));
        }
    }

    public static void main(String[] args) {
        try {

            String input = Config.MOBILE_DPI_INPUT;
            String output = Config.MOBILE_CDR_OUTPUT;
            if (null != args && args.length != 0) {
                if (args.length == 2) {
                    input = args[0];
                    output = args[1];
                } else {
                    JobUtils.exit("the num of parameter is illegal.");
                }
            }

            Configuration cf = MrUtils.getRaiyiConfiguration();
            Job job = Job.getInstance(cf, "dpiJob");
            job.setJarByClass(MatchMblHostComplain.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(mblDpiHostMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(dpiHostReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


            // MultipleInputs类添加文件路径
            FileInputFormat.addInputPath(job, new Path(input));

            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.out.println(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
