package com.jihf.mr.mapReduce.complainModel;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.JobUtils;
import com.jihf.mr.utils.MrUtils;
import com.raiyi.modelV2.Complaint;
import com.raiyi.modelV2.FlowAnalysis;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Func：投诉电话数据
 * Desc:
 * Author：JHF
 * Data：2017-09-08 10:54
 * Mail：jihaifeng@raiyi.com
 */
public class EtlData {
    public static final String TAG_1 = "1";
    public static final String TAG_2 = "2";

    /**
     * 投诉电话数据源
     */
    public static class etlDataMap extends Mapper<AvroKey<FlowAnalysis>, NullWritable, Text, Text> {
        @Override
        protected void map(AvroKey<FlowAnalysis> key, NullWritable value, Context context) throws IOException, InterruptedException {
            try {
                Complaint complaint = key.datum().getComplaintRatio();
                String mobile = key.datum().getMobile().toString();
                String basicFee = key.datum().getBasicFee().toString();
                int callFromTel = complaint.getCallFromTele(); //
                int callToTel = complaint.getCallToTele();
                int callToSms = complaint.getCallToSms();
                int callToMit = complaint.getCallToMIT();
                String queryDate = key.datum().getQueryDate().toString();
                if (callFromTel > 0 || callToTel > 0) {
                    context.write(new Text(mobile), new Text(String.format("%s|%s|%s|%s",
                            queryDate,
                            callFromTel,
                            callToTel,
                            basicFee)));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 输出 手机号|拨打投诉电话次数|接听投诉电话次数
     */
    public static class etlDataReduce extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                long maxTime = -1;
                String callTo = "-1";
                String callFrom = "-1";
                String basicFee = "-1";
                for (Text t : values) {
                    String[] datas = t.toString().split("\\|", -1);
                    String queryDate = datas[0];
                    long dateTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(queryDate).getTime();
                    if (dateTime > maxTime) {
                        callTo = datas[1];
                        callFrom = datas[2];
                        basicFee = datas[3];
                    }

                }
                context.write(NullWritable.get(), new Text(String.format("%s|%s|%s|%s", key, callTo, callFrom,basicFee)));
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

    }

    public static void main(String[] args) {
        try {
            String input = "jihaifeng/testNum/mobile_20170825_cloud812.txt";
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
            job.setJarByClass(EtlData.class);

            job.setInputFormatClass(AvroKeyInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(etlDataMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(etlDataReduce.class);
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
    }
}
