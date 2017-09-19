package com.jihf.mr.mapReduce.complainModel;

import com.jihf.mr.constants.Config;
import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataBean;
import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataUtils;
import com.jihf.mr.mapReduce.hiveSMSData.HiveSmsDataBean;
import com.jihf.mr.mapReduce.hiveSMSData.HiveSmsDataUtils;
import com.jihf.mr.utils.*;
import com.raiyi.modelV2.Complaint;
import com.raiyi.modelV2.FlowAnalysis;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-08 10:54
 * Mail：jihaifeng@raiyi.com
 */
public class ComplainData extends Configured implements Tool {
    public static final String TAG_test = "0";
    public static final String TAG_FLOW = "1";
    public static final String TAG_SMS = "2";
    public static final String TAG_ETL = "3";

    public static List<String> pathList = new ArrayList<String>();

    @Override
    public int run(String[] args) throws Exception {
        try {
            String input1 = "jihaifeng/complain/dw_flow";
            String input2 = "jihaifeng/complain/sms_0804";
            String input3 = "jihaifeng/complain/etl_0804";
            String output = Config.MOBILE_DPI_OUTPUT;
            if (null != args && args.length != 0) {
                if (args.length == 4) {
                    input1 = args[0];
                    input2 = args[1];
                    input3 = args[2];
                    output = args[3];
                } else {
                    JobUtils.exit("the num of parameter is illegal.");
                }
            }
            Configuration cf = MrUtils.getRaiyiConfiguration();
            Job job = Job.getInstance(cf, "hiveData");
            job.setJarByClass(ComplainData.class);

            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(complainDataReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


            // MultipleInputs类添加文件路径
//            MultipleInputs.addInputPath(job, new Path(input1),
//                    TextInputFormat.class, flowDataMap.class);
//            MultipleInputs.addInputPath(job, new Path(input2),
//                    TextInputFormat.class, smsDataMap.class);
//            MultipleInputs.addInputPath(job, new Path(input3),
//                    AvroKeyInputFormat.class, etlDataMap.class);

            HDFSFileUtils.initInputPath(cf, job, input1, TextInputFormat.class, flowDataMap.class);
            HDFSFileUtils.initInputPath(cf, job, input2, TextInputFormat.class, smsDataMap.class);
            HDFSFileUtils.initInputPath(cf, job, input3, AvroKeyInputFormat.class, etlDataMap.class);


            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输入目录：" + input2);
            System.out.println("输入目录：" + input3);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.out.println(job.waitForCompletion(true) ? 0 : 1);


        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return 0;
    }


    /**
     * 流量数据
     */
    public static class flowDataMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\u0001", -1);
            HiveFlowDataBean flowDataBean = HiveFlowDataUtils.parse2FlowBean(datas);
            int day = Integer.parseInt(flowDataBean.date.substring(8, flowDataBean.date.length())) - 1;
            context.write(new Text(flowDataBean.mobile),
                    new Text(String.format("%s|%s|%s|%s|%s",
                            TAG_FLOW,
                            flowDataBean.flow_total,
                            flowDataBean.flow_used,
                            flowDataBean.main_price,
                            day)));
        }
    }

    /**
     * 短信数据
     */
    public static class smsDataMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] datas = value.toString().split("\u0001", -1);
            HiveSmsDataBean smsDataBean = HiveSmsDataUtils.parse2SmsBean(datas);
            String data = TimeUtils.stampToDate(datas[24]);
            int day = Integer.parseInt(data.substring(8, 10));
            if (smsDataBean.sms_type.equals("3")) {
                context.write(new Text(smsDataBean.mobile),
                        new Text(String.format("%s|%s",
                                TAG_SMS,
                                day)));
            }
        }
    }

    /**
     * 投诉电话数据
     */
    public static class etlDataMap extends Mapper<AvroKey<FlowAnalysis>, NullWritable, Text, Text> {
        @Override
        protected void map(AvroKey<FlowAnalysis> key, NullWritable value, Context context) throws IOException, InterruptedException {
            Complaint complaint = key.datum().getComplaintRatio();
            String mobile = key.datum().getMobile().toString();
            String basicFee = key.datum().getBasicFee().toString();
            int callFromTel = complaint.getCallFromTele();
            int callToTel = complaint.getCallToTele();
            String queryDate = key.datum().getLogDate().toString();
            int day = Integer.parseInt(queryDate.substring(5, 8));
            context.write(new Text(mobile), new Text(String.format("%s|%s|%s|%s|%s|%s",
                    TAG_ETL,
                    queryDate,
                    callFromTel,
                    callToTel,
                    basicFee,
                    day)));
        }
    }

    /**
     * flow_query_record表，http://61.155.179.141:8889/hue/editor/?type=hive
     */
    public static class complainDataReduce extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs output;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            output = new MultipleOutputs(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 流量数据
            int maxFlowDay = -1;
            int minFlowDay = -1;
            double flowTotal = -1;
            double flowUsed = -1;
            int flowDurDay = -1;
            double mainPrice = -1;

            // 短信数据
            int smsCount = 0;
            int minSmsDay = -1;
            int maxSmsDay = -1;
            int smsDurDay = -1;

            // 电话数据
            long maxTime = -1;
            int callTo = -1;
            int callFrom = -1;
            double basicFee = -1;
            int minCallDay = -1;
            int maxCallDay = -1;
            int callDurDay = -1;

            for (Text val : values) {
                String[] datas = val.toString().split("\\|", -1);
                String tag = datas[0];

                // 流量数据
                if (tag.equals(TAG_FLOW)) {
                    int _day = Integer.parseInt(datas[4]);
                    if (minFlowDay == -1 || minFlowDay > _day) {
                        minFlowDay = _day;
                    }
                    if (maxFlowDay == -1 || maxFlowDay < _day) {
                        maxFlowDay = _day;
                        flowTotal = StringUtils.strIsEmpty(datas[1]) ? -1 : Double.parseDouble(datas[1]);
                        flowUsed = StringUtils.strIsEmpty(datas[2]) ? -1 : Double.parseDouble(datas[2]);
                        mainPrice = StringUtils.strIsEmpty(datas[3]) ? -1 : Double.parseDouble(datas[3]);
                    }
                    if (flowTotal > 0 || flowUsed > 0 || mainPrice > 0) {
                        output.write(NullWritable.get(), new Text(String.format("%s|%s|%s|%s",
                                key,
                                flowTotal,
                                flowUsed,
                                mainPrice)), "flow");
                    }
                }

                // 短信数据
                if (tag.equals(TAG_SMS)) {
                    int _day = Integer.parseInt(datas[1]);

                    if (minSmsDay == -1 || minSmsDay > _day) {
                        minSmsDay = _day;
                    }

                    if (maxSmsDay == -1 || maxSmsDay < _day) {
                        maxSmsDay = _day;
                    }
                    smsCount++;

                }

                // 电话数据
                if (tag.equals(TAG_ETL)) {
                    try {
                        String queryDate = datas[1];
                        long dateTime = new SimpleDateFormat("yyyyMMdd").parse(queryDate).getTime();
                        if (dateTime > maxTime) {
                            callTo = StringUtils.strIsEmpty(datas[2]) ? -1 : Integer.parseInt(datas[2]);
                            callFrom = StringUtils.strIsEmpty(datas[3]) ? -1 : Integer.parseInt(datas[3]);
                            basicFee = StringUtils.strIsEmpty(datas[4]) ? -1 : Double.parseDouble(datas[4]);
                            maxTime = dateTime;
                        }
                        int _day = Integer.parseInt(datas[5]);

                        if (minCallDay == -1 || minCallDay > _day) {
                            minCallDay = _day;
                        }

                        if (maxCallDay == -1 || maxCallDay < _day) {
                            maxCallDay = _day;
                        }
                        output.write(NullWritable.get(), new Text(String.format("%s|%s|%s",
                                key,
                                callTo,
                                callFrom)), "etl");
                    } catch (ParseException e) {
                        output.write(NullWritable.get(), new Text(e.toString()), "err");
                        e.printStackTrace();
                    }
                }
            }

            flowDurDay = mathDurDay(minFlowDay, maxFlowDay);
            smsDurDay = mathDurDay(minSmsDay, maxSmsDay);
            callDurDay = mathDurDay(minCallDay, maxCallDay);

            long orderSize = 3072;
            long orderPrice = 100;

            long A = isUp0(callTo) ? callTo : 0;
            long B = isUp0(callFrom) ? callFrom : 0;
            double C = isUp0(flowDurDay, flowTotal, flowUsed) && (flowUsed * 30 / flowDurDay > flowTotal) ? (1 - (flowUsed * 30 / flowDurDay - flowTotal) / orderSize) : 0;
            double D = isUp0(orderPrice, basicFee, callDurDay, mainPrice) ? orderPrice / (basicFee * 30 / (callDurDay * 1000) + mainPrice) : 0;
            long E = isUp0(smsCount, smsDurDay) ? smsDurDay / smsCount : 0;
            long F = 0;

//            s1 = IF(B>0,MAX(8*A*B,0),MAX(3*A,0))
//            s2 = MAX(10*B,0)
//            s3 = MAX(10*C,0)
//            s4 = IF(D>0.6,20*D,IF(D>0.3,10*D,MAX(5*D,0)))
//            s5 = IF(E<1,0,MAX((4-E)*10,0))
//            s6 = IF(E<=1,0,MAX(10*(A- 2)*(4-E),0))
//            s7 = MAX(20*(F-2),0)

            long s1 = isUp0(B) ? 8 * A * B : 3 * A;
            long s2 = 10 * B;
            double s3 = 10 * C;
            double s4 = D > 0.6 ? 20 * D : (D > 0.3 ? 10 * D : 5 * D);
            long s5 = E < 1 ? 0 : (E < 4 ? (4 - E) * 10 : 0);
            long s6 = E <= 1 ? 0 : (A > 2 && E < 4 ? 10 * (A - 2) * (4 - E) : 0);
            long s7 = F > 2 ? 20 * (F - 2) : 0;
            double score = s1 + s2 + s3 + s4 + s5 + s6 + s7;
            if (!StringUtils.strIsEmpty(key.toString()) && score > 0) {
                context.write(NullWritable.get(), new Text(String.format("%s|%s|%s|%s|%s|%s|%s|%s",
                        key,
                        A,
                        B,
                        C,
                        D,
                        E,
                        F,
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

        private boolean isUp0(double... num) {
            boolean flag = true;
            for (double n : num) {
                if (n <= 0) {
                    flag = false;
                }
            }
            return flag;
        }

        private int mathDurDay(int minDay, int maxDay) {
            return maxDay < minDay ? -1 : (maxDay - minDay);
        }

    }


    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new ComplainData(), args);
        System.exit(exitCode);

    }
}
