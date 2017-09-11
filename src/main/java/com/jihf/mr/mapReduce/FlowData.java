package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataBean;
import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataUtils;
import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.JobUtils;
import com.jihf.mr.utils.MrUtils;
import com.sun.tools.javac.util.Convert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-08 10:54
 * Mail：jihaifeng@raiyi.com
 */
public class FlowData {
    public static final String TAG_1 = "1";
    public static final String TAG_2 = "2";

    public static class phoneNumMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text(TAG_1));
        }
    }

    public static class flowDataMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] datas = value.toString().split("\u0001", -1);
            HiveFlowDataBean flowDataBean = HiveFlowDataUtils.parse2FlowBean(datas);
//            int day = flowDataBean.date.substring();
            try {
                int day =  Integer.parseInt(flowDataBean.date.substring(8,flowDataBean.date.length()));
                 context.write(new Text(flowDataBean.mobile),
                        new Text(String.format("%s|%s|%s|%s|%s|%s",
                                flowDataBean.mobile,
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

    public static class flowDataReduce extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean flag1 = false;
            boolean flag2 = false;
            String flowTotal = null;
            List<String> valData = new ArrayList<String>();
            for (Text val : values) {
                String[] tt = val.toString().split("\\|", -1);
                if (tt.length == 1) {
                    flag1 = true;
                }
                if (tt.length == 6) {
                    flag2 = true;
                    valData.add(val.toString());
//                    context.write(NullWritable.get(),val);
                }

            }
            if (flag1 && flag2) {
                for (String val : valData) {
                    context.write(NullWritable.get(), new Text(val));
                }

            }

        }
    }


    public static void main(String[] args) {
        try {
            String input1 = "jihaifeng/testNum/dw_flow_phone.txt";
            String input2 = "jihaifeng/testNum/dw_flow";
            String output = Config.MOBILE_DPI_OUTPUT;
            if (null != args && args.length != 0) {
                if (args.length == 3) {
                    input1 = args[0];
                    input2 = args[1];
                    output = args[2];
                } else {
                    JobUtils.exit("the num of parameter is illegal.");
                }
            }
            Configuration cf = MrUtils.getRaiyiConfiguration();
            Job job = Job.getInstance(cf, "hiveData");
            job.setJarByClass(FlowData.class);

            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(flowDataReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


            // MultipleInputs类添加文件路径
            MultipleInputs.addInputPath(job, new Path(input1),
                    TextInputFormat.class, phoneNumMap.class);
            MultipleInputs.addInputPath(job, new Path(input2),
                    TextInputFormat.class, flowDataMap.class);

            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输入目录：" + input2);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.out.println(job.waitForCompletion(true) ? 0 : 1);


        } catch (Exception e) {
            System.out.println(e.getMessage());
        }


    }
}
