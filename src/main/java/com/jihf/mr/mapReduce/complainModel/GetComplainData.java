package com.jihf.mr.mapReduce.complainModel;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.*;
import com.raiyi.modelV2.Complaint;
import com.raiyi.modelV2.FlowAnalysis;
import model.DpiResult;
import model.HostPair;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-18 14:55
 * Mail：jihaifeng@raiyi.com
 */
public class GetComplainData extends Configured implements Tool {
    public static final String TAG_URL = "1";
    public static final String TAG_ETL = "2";
    public static List<String> pathList = new ArrayList<String>();

    @Override
    public int run(String[] args) throws Exception {
        //  hadoop jar datac-1.16-shaded.jar  complainData <Dpi数据  etl数据  输出目录>
        String input1 = "jihaifeng/testComplain/jiangsu_0904.avro";
        String input2 = "jihaifeng/testComplain/mobile_20170912_cloud832.txt";
        String output = Config.MOBILE_DPI_OUTPUT;
        if (null != args && args.length != 0) {
            if (args.length == 2) {
                input1 = args[0];
                input2 = args[1];
            } else if (args.length == 3) {
                input1 = args[0];
                input2 = args[1];
                output = args[2];
            } else {
                JobUtils.exit("the num of parameter is illegal.");
            }
        }
        Configuration cf = MrUtils.getRaiyiConfiguration();
        Job job = Job.getInstance(cf, "hiveData");
        job.setJarByClass(GetComplainData.class);


        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(complainDataReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        // MultipleInputs类添加文件路径
//        MultipleInputs.addInputPath(job, new Path(input1),
//                AvroKeyInputFormat.class, dpiDataMap.class);
//        MultipleInputs.addInputPath(job, new Path(input2),
//                AvroKeyInputFormat.class, etlDataMap.class);

        HDFSFileUtils.initInputPath(cf, job, input1, AvroKeyInputFormat.class, dpiDataMap.class);
        HDFSFileUtils.initInputPath(cf, job, input2, AvroKeyInputFormat.class, etlDataMap.class);


        FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

        System.out.println("\n==================================\n");
        System.out.println("输入目录：" + input1);
        System.out.println("输入目录：" + input2);
        System.out.println("输出目录：" + output);
        System.out.println("\n==================================\n");

        System.out.println(job.waitForCompletion(true) ? 0 : 1);

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

    /**
     * 投诉电话数据
     * http://61.155.179.141:8889/hue/metastore/table/etl_output/flow_result
     * etl_output.flow_result
     */
    public static class etlDataMap extends Mapper<AvroKey<FlowAnalysis>, NullWritable, Text, Text> {
        @Override
        protected void map(AvroKey<FlowAnalysis> key, NullWritable value, Context context) throws IOException, InterruptedException {
            Complaint complaint = key.datum().getComplaintRatio();

            int callTo = null != complaint ? complaint.getCallFromTele() : 0;
            int callFrom = null != complaint ? complaint.getCallToTele() : 0;

            String mobile = StringUtils.strIsEmpty(key.datum().getMobile().toString()) ? null : key.datum().getMobile().toString();
            String queryDate = StringUtils.strIsEmpty(key.datum().getQueryDate().toString()) ? null : key.datum().getQueryDate().toString();
            String basicFee = StringUtils.strIsEmpty(key.datum().getBasicFee().toString()) ? null : key.datum().getBasicFee().toString();

            if (!StringUtils.strIsEmpty(mobile) && (callFrom > 0 || callTo > 0)) {
                context.write(new Text(mobile), new Text(String.format("%s|%s|%s|%s|%s",
                        TAG_ETL,
                        queryDate,
                        callTo,
                        callFrom,
                        basicFee)));
            }
        }
    }

    /**
     * reduce
     */
    public static class complainDataReduce extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            boolean flag1 = false;
            boolean flag2 = false;
            // DPI域名访问次数
            int urlCount = 0;

            // 电话数据
            long maxTime = -1;
            int callTo = -1;
            int callFrom = -1;
            long basicFee = -1;

            for (Text val : values) {
                String[] datas = val.toString().split("\\|", -1);
                String tag = datas[0];

                // DPI数据
                if (tag.equals(TAG_URL)) {
                    urlCount += Integer.parseInt(datas[2]);
                    flag1 = true;
                }

                // 电话数据
                if (tag.equals(TAG_ETL)) {
                    try {
                        String queryDate = datas[1];
                        long dateTime = !StringUtils.strIsEmpty(queryDate) ? new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(queryDate).getTime() : -1;
                        if (dateTime > maxTime) {
                            callTo = StringUtils.strIsEmpty(datas[2]) ? -1 : Integer.parseInt(datas[2]);
                            callFrom = StringUtils.strIsEmpty(datas[3]) ? -1 : Integer.parseInt(datas[3]);
                            maxTime = dateTime;
                        }
                        basicFee = StringUtils.strIsEmpty(datas[4]) ? -1 : Long.parseLong(datas[4]);
                        flag2 = true;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            long A = isUp0(callTo) ? callTo : 0;
            long B = isUp0(callFrom) ? callFrom : 0;
            long C = isUp0(urlCount) ? urlCount : 0;

            long s1 = isUp0(B) ? 10 * A * B : 5 * A;
            long s2 = 10 * B;
            long s3 = 10 * C;
            long s4 = isUp0(A, C) ? 10 * A * C : 0;
            double score = s1 + s2 + s3 + s4;
            if (!StringUtils.strIsEmpty(key.toString()) && flag1 && flag2) {
                context.write(NullWritable.get(), new Text(String.format("%s|%s|%s|%s|%s",
                        key,
                        A,
                        B,
                        C,
                        score)));
            }
        }

        private boolean isUp0(long... num) {
            boolean flag = true;
            for (long n : num) {
                if (n <= 0) {
                    flag = false;
                }
            }
            return flag;
        }

    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GetComplainData(), args);
        System.exit(exitCode);
    }
}
