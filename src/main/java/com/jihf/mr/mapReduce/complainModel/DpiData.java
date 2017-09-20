package com.jihf.mr.mapReduce.complainModel;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.*;
import model.DpiResult;
import model.HostPair;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-18 13:30
 * Mail：jihaifeng@raiyi.com
 */
public class DpiData extends Configured implements Tool {
    public static final String TAG_URL = "1";

    @Override
    public int run(String[] args) throws Exception {
        try {
            //  hadoop jar datac-1.16-shaded.jar  complainData <样本手机号  Dpi数据  etl数据  输出目录>
            String input = "jihaifeng/20170909/jiangsu_dpi_0909.avro";
            String output = Config.MOBILE_DPI_OUTPUT;
            if (null != args && args.length != 0) {
                if (args.length == 2) {
                    input = args[0];
                    output = args[1];
                } else {
                    JobUtils.exit("the num of parameter is illegal.");
                }
            }
            Configuration cf = MrUtils.getRaiyiConfiguration();
            Job job = Job.getInstance(cf, "hiveData");
            job.setJarByClass(MatchComplainData.class);

            job.setInputFormatClass(AvroKeyInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(dpiDataMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(dpiDataReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.out.println(job.waitForCompletion(true) ? 0 : 1);


        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return 0;
    }

    /**
     * Hive上的dpi数据
     * http://61.155.179.141:8889/hue/metastore/table/etl_output/dpi_result
     * etl_output.dpi_result
     */
    public static class dpiDataMap extends Mapper<AvroKey<DpiResult>, NullWritable, Text, Text> {
        Matcher matcher = new Matcher(true);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            matcher.addPattern("JBZS.12321.CN", 100);
            matcher.addPattern("C.INTERFACE.GOOTION.COM", 200);
            matcher.addPattern("12321.CN", 300);
            matcher.addPattern("110.360.CN", 400);
            matcher.addPattern("C.INTERFACE.AT321.CN", 500);
            matcher.addPattern("DATA.HAOMA.SOGOU.COM", 600);
        }

        @Override
        protected void map(AvroKey<DpiResult> key, NullWritable value, Context context) throws IOException, InterruptedException {

            String phone_number = null != key.datum().getPhoneNumber() ? key.datum().getPhoneNumber().toString() : null;
            String device_id = null != key.datum().getDeviceId() ? key.datum().getDeviceId().toString() : null;
            int province = key.datum().getProvince();
            String log_date = null != key.datum().getLogDate() ? key.datum().getLogDate().toString() : null;
            String user_agent = null != key.datum().getUserAgent() ? key.datum().getUserAgent().toString() : null;
            List<HostPair> hostPairList = key.datum().getHostFreq();
            for (HostPair hostPair : hostPairList) {
                if (matchChe(hostPair.getHost().toString().toUpperCase())) {
                    context.write(new Text(phone_number), new Text(String.format("%s|%s|%s", TAG_URL, hostPair.getHost(), hostPair.getFrequency())));
                }
            }
        }

        private boolean matchChe(String url) {

            if (StringUtils.strIsEmpty(url)) {
                return false;
            }
            if (!StringUtils.strIsEmpty(url)) {
                Matcher.MatchResult[] a = matcher.match(url);
                if (a.length != 0) {
                    System.out.println(url);
                    return true;
                }
            }
            return false;
        }
    }

    public static class dpiDataReduce extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text tt : values) {
                context.write(NullWritable.get(), new Text(String.format("%s|%s", key, tt)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DpiData(), args);
        System.exit(exitCode);
    }
}
