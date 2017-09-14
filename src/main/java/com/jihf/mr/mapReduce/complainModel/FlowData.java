package com.jihf.mr.mapReduce.complainModel;

import com.jihf.mr.constants.Config;
import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataBean;
import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataUtils;
import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.JobUtils;
import com.jihf.mr.utils.MrUtils;
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
public class FlowData {

    public static class flowDataMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\u0001", -1);
            HiveFlowDataBean flowDataBean = HiveFlowDataUtils.parse2FlowBean(datas);
            try {
                int day = Integer.parseInt(flowDataBean.date.substring(8, flowDataBean.date.length())) - 1;
                context.write(new Text(flowDataBean.mobile),
                        new Text(String.format("%s|%s|%s|%s|%s",
                                flowDataBean.flow_total,
                                flowDataBean.flow_over,
                                flowDataBean.flow_used,
                                flowDataBean.main_price,
                                day)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * flow_query_record表，http://61.155.179.141:8889/hue/editor/?type=hive
     */
    public static class flowDataReduce extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxDay = -1;
            int minDay = -1;
            String flowTotal = "-1";
            String flowUsed = "-1";
            String mainPrice = "-1";
            for (Text val : values) {
                String[] datas = val.toString().split("\\|", -1);
                int _day = Integer.parseInt(datas[5]);
                if (minDay == -1 || minDay > _day) {
                    minDay = _day;
                }
                if (maxDay == -1 || maxDay < _day) {
                    maxDay = _day;
                    flowTotal = datas[0];
                    flowUsed = datas[2];
                    mainPrice = datas[3];
                }
            }
            int durDay = (maxDay - minDay) < 0 ? -1 : (maxDay - minDay);
            context.write(NullWritable.get(), new Text(String.format("%s|%s|%s|%s|%s", key, flowTotal, flowUsed, durDay,mainPrice)));

        }

    }


    public static void main(String[] args) {
        try {
            String input = "jihaifeng/testNum/dw_flow";
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
            job.setJarByClass(FlowData.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(flowDataMap.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(flowDataReduce.class);
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
