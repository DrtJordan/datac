package com.jihf.mr.mapReduce.complainModel;

import com.jihf.mr.constants.Config;
import com.jihf.mr.mapReduce.hiveSMSData.HiveSmsDataBean;
import com.jihf.mr.mapReduce.hiveSMSData.HiveSmsDataUtils;
import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.JobUtils;
import com.jihf.mr.utils.MrUtils;
import com.jihf.mr.utils.TimeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 * Data：2017-09-08 10:54
 * Mail：jihaifeng@raiyi.com
 */
public class SmsData {
    public static final String TAG_1 = "1";
    public static final String TAG_2 = "2";

    public static class smsDataMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            try {
                String[] datas = value.toString().split("\u0001", -1);
                HiveSmsDataBean smsDataBean = HiveSmsDataUtils.parse2SmsBean(datas);
                String data = TimeUtils.stampToDate(datas[24]);
                int day = Integer.parseInt(data.substring(8, 10));
                if (smsDataBean.sms_type.equals("3")) {
                    context.write(new Text(smsDataBean.mobile),
                            new Text(String.format("%s|%s",
                                    data,
                                    day)));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * flow_query_record表，http://61.155.179.141:8889/hue/editor/?type=hive
     */
    public static class smsDataReduce extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int minDay = -1;
            int maxDay = -1;
            String maxData = null;
            String minData = null;
            for (Text text : values) {
                String[] _tempData = text.toString().split("\\|", -1);
                int _day = Integer.parseInt(_tempData[1]);

                if (minDay == -1 || minDay > _day) {
                    minDay = _day;
                    minData = _tempData[0];
                }
                if (maxDay == -1 || maxDay < _day) {
                    maxDay = _day;
                    maxData = _tempData[0];
                }
                count++;
            }

            int durDay = (maxDay - minDay) < 0 ? -1 : (maxDay - minDay);
            context.write(NullWritable.get(),
                    new Text(String.format("%s|%s|%s"
                            , key
                            , count
                            , durDay)));

        }
    }


    public static void main(String[] args) {
        try {
            String input = "jihaifeng/testNum/dw_sms";
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
            job.setJarByClass(SmsData.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(smsDataMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(smsDataReduce.class);
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
