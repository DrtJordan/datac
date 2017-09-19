package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.mapReduce.sort.ComparatorKey;
import com.jihf.mr.mapReduce.sort.VisitInfo;
import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.JobUtils;
import com.jihf.mr.utils.MrUtils;
import com.jihf.mr.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Func：统计移网DPI数据中用户访问网站的域名以及访问次数
 * Desc: job取数据 ， job2按照访问数排序
 * Author：JHF
 * Data：2017-08-09 09:26
 * Mail：jihaifeng@raiyi.com
 */
public class MblDpiHostSort extends Configured implements Tool {
    private static final IntWritable NUM = new IntWritable(1);

    private static final String MAX_OUTPUT_NUM = "maxOutputNum";
    private static final String MIN_VISIT_TIMES = "minVisitTimes";

    @Override
    public int run(String[] strings) throws Exception {
        // 判空
        String input = Config.MOBILE_DPI_INPUT;
        String output = Config.MOBILE_DPI_OUTPUT;

        Configuration cf = MrUtils.getRaiyiConfiguration();

        int index = 0;
        if (null != strings && strings.length != 0) {
            if (StringUtils.strIsEmpty(strings)) {
                JobUtils.exit("please input params like <mblDpiInput resultOutput 【choose：outputDataLength minVisitTimes】>");
            }
            if (strings.length == 2) {
                input = strings[index++];
                output = strings[index];
            } else if (strings.length == 3) {
                input = strings[index++];
                output = strings[index++];
                DefaultStringifier.store(cf, new Text(StringUtils.strIsEmpty(strings[index]) ? "100" : strings[index]), MAX_OUTPUT_NUM);
            } else if (strings.length == 4) {
                input = strings[index++];
                output = strings[index++];
                DefaultStringifier.store(cf, new Text(StringUtils.strIsEmpty(strings[index]) ? "100" : strings[index++]), MAX_OUTPUT_NUM);
                DefaultStringifier.store(cf, new Text(StringUtils.strIsEmpty(strings[index]) ? "0" : strings[index]), MIN_VISIT_TIMES);
            } else {
                JobUtils.exit("please input params like <mblDpiInput resultOutput 【choose：outputDataLength minVisitTimes】>");
            }
        }


        try {
            Job job = Job.getInstance(cf);
            job.setJobName("hostSort");

            // 通过class名称查找jar包
            job.setJarByClass(MblDpiHostSort.class);

            // 设置输入文件按照什么格式被读取
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);


            // 自定义Map
            job.setMapperClass(mblDpiMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            // 自定义reduce
            job.setReducerClass(mblDpiReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);


            // 设置输入文件夹
            FileInputFormat.addInputPath(job, new Path(input));

            // 设置输出文件夹
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            if (job.waitForCompletion(true)) {
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            }
        } catch (Exception e) {
            JobUtils.exit(e.getMessage());
            e.printStackTrace();
        }
        return 0;
    }

    public static class mblDpiMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String data = value.toString();
            if (StringUtils.strIsEmpty(data)) {
                JobUtils.exit("input data is null.");
            }

            String[] datas = data.split("\\|", -1);
            if (datas.length < 40) {
                JobUtils.exit("input data has missed some field.");
            }

            String domainName = datas[30];
            String msisdn = datas[1];
            if (StringUtils.strIsNotEmpty( domainName)) {
                context.write(new Text(domainName), NUM);
            }

        }
    }


    // 如果输入输出值的类型不是默认，需要完善泛型
    public static class mblDpiReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
        List<VisitInfo> visitList = new ArrayList<VisitInfo>();
        // 自定义排序
        ComparatorKey comparator = new ComparatorKey();
        //选取访问网站次数排名前K位的url
        int max_k = 100;
        //低于 min_count 的值不输出
        int min_count = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            max_k = Integer.parseInt(DefaultStringifier.load(conf, MAX_OUTPUT_NUM, Text.class).toString());
            min_count = Integer.parseInt(DefaultStringifier.load(conf, MIN_VISIT_TIMES, Text.class).toString());
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String url = key.toString();

            long count = 0;
            for (IntWritable val : values) {
                count++;
            }
            if (count > 0) {

                if (visitList.size() < max_k && count > min_count) {
                    if (!StringUtils.strIsEmpty(url)) {
                        VisitInfo visitInfo = new VisitInfo(count, url);
                        visitList.add(visitInfo);
                    }
                }

                //边界值处理
                if (visitList.size() >= max_k && count > min_count) {
                    // 先排序
                    Collections.sort(visitList, comparator);
                    // 求出访问最小的一项
                    VisitInfo min = visitList.get(visitList.size() - 1);
                    Long minCount = min.getKey();//最小的值
                    if (count > minCount) {
                        //删除最小的  
                        visitList.remove(visitList.size() - 1);
                        //添加大的                    
                        visitList.add(new VisitInfo(count, key.toString()));
                    }

                }

                if (!context.nextKey()) {
                    Collections.sort(visitList, comparator);
                    for (VisitInfo info : visitList) {
                        context.write(NullWritable.get(), new Text(String.format("%s|%s", info.getKey(), info.getVal())));
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new MblDpiHostSort(), args);
        System.exit(result);
    }


}
