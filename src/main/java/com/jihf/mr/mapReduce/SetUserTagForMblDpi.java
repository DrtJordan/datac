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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-22 18:34
 * Mail：jihaifeng@raiyi.com
 */
public class SetUserTagForMblDpi extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        String input = Config.MOBILE_DPI_INPUT;
        String output = Config.MOBILE_DPI_OUTPUT;
        if (null != strings && strings.length != 0) {
            if (strings.length == 1) {
                input = strings[0];
            } else if (strings.length == 2) {
                input = strings[0];
                output = strings[1];
            } else {
                JobUtils.exit("the num of parameter is illegal.");
            }
        }
        if (!StringUtils.strIsNotEmpty(input, output)) {
            JobUtils.exit("the parameter is illegal.");
        }
        Configuration cf = MrUtils.getRaiyiConfiguration();
        Job job = Job.getInstance(cf, "userTag");
        job.setJarByClass(SetUserTagForMblDpi.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(mblDpiMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(userTagReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(input));

        FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(cf, output));

        System.out.println("\n==================================\n");
        System.out.println("输入目录：" + input);
        System.out.println("输出目录：" + output);
        System.out.println("\n==================================\n");

        System.out.println(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SetUserTagForMblDpi(), args);
        System.exit(exitCode);
    }

    public static class mblDpiMapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, String> tagMap = new HashMap<>();
        Matcher matcher = new Matcher(true);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            tagMap = readTagDataFile(tagMap, matcher);
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] datas = DpiDataUtils.getSplitData(value, 38);
            if (null != datas) {
                String msisdn = datas[1];
                String endTime = StringUtils.strIsNotEmpty(datas[18]) ? datas[18] : null;
                String duration = datas[19];
                long inputOctets = StringUtils.strIsNotEmpty(datas[20]) ? Long.parseLong(datas[20]) : 0;
                long outputOctets = StringUtils.strIsNotEmpty(datas[21]) ? Long.parseLong(datas[21]) : 0;
                String ua = datas[28];
                String url = datas[29];
                String domainName = datas[30];
                String host = datas[31];
                if (StringUtils.strIsNotEmpty(url, msisdn)) {
                    String hostName = domainName;
                    if (StringUtils.strIsEmpty(hostName) && StringUtils.strIsNotEmpty(host)) {
                        hostName = host;
                    }
                    if (StringUtils.strIsNotEmpty(hostName)) {
                        Matcher.MatchResult[] matchResults = matcher.match(hostName.toLowerCase());
                        if (matchResults.length != 0) {
                            for (Matcher.MatchResult result : matchResults) {
                                String tagCode = tagMap.get(result.pattern);
                                context.write(new Text(String.format("%s|%s|%s", msisdn, tagCode, hostName)),
                                        new Text(String.format("%s|%s|%s|%s|%s"
                                                , url
                                                , duration
                                                , inputOctets + outputOctets
                                                , result.pattern
                                                , endTime)));
                            }
                        }

                    }

                }

            }
        }

        private HashMap<String, String> readTagDataFile(HashMap<String, String> map, Matcher matcher) {
            if (null == map) {
                map = new HashMap<>();
            }
            if (map.size() != 0) {
                return null;
            }
            try {
                InputStream in = SetUserTagForMblDpi.class.getClassLoader().getResourceAsStream("bd_tag_host.txt");
                InputStreamReader isr = new InputStreamReader(in);
                BufferedReader br = new BufferedReader(isr);
                String lineNext = null;
                int i = 0;
                while ((lineNext = br.readLine()) != null) {
                    String[] datas = DpiDataUtils.getSplitData(new Text(lineNext), 2);
                    if (null != datas) {
                        String tagCode = StringUtils.strIsNotEmpty(datas[0]) ? datas[0] : null;
                        String hostName = StringUtils.strIsNotEmpty(datas[1]) ? datas[1] : null;
                        String link =  StringUtils.strIsNotEmpty(datas[2]) ? datas[2] : null;
                        if (StringUtils.strIsNotEmpty(tagCode, link)) {
                            String tagKey = String.format("%s%s%s", "*.", link, "*").toLowerCase();
                            String tagVal = tagCode;
                            map.put(tagKey, tagVal);
                            i++;
                            matcher.addPattern(tagKey, i * 100);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return map;
        }
    }

    public static class userTagReduce extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keys = key.toString().split("\\|", -1);
            int tagLevel = 0;
            String msisdn = keys[0];
            String tagCode = keys[1];
            String host = keys[2];

            String time = null;
            long duration = 0;
            long visitOctets = 0;
            List<String> urlList = new ArrayList<>();

            List<String> hostMatchList = new ArrayList<>();
            for (Text val : values) {
                String[] datas = DpiDataUtils.getSplitData(val, 3);
                tagLevel++;
                if (null != datas) {
                    if (!urlList.contains(datas[0])) {
                        urlList.add(datas[0]);
                    }
                    duration += Long.parseLong(datas[1]);
                    visitOctets += Long.parseLong(datas[2]);
                    if (!hostMatchList.contains(datas[3])) {
                        hostMatchList.add(datas[3]);
                    }
                    if (StringUtils.strIsNotEmpty(datas[4])) {
                        time = datas[4];
                    }
                }

            }
            // 设置日期格式
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            if (StringUtils.strIsEmpty(time)) {
                // new Date()为获取当前系统时间
                time = df.format(new Date());
            } else {
                try {
                    time = df.format(new SimpleDateFormat("yyyyMMddHHmmss").parse(time));
                } catch (ParseException e) {
                    time = df.format(new Date());
                    e.printStackTrace();
                }
            }
            StringBuffer sql = new StringBuffer("INSERT INTO \"public\".\"bd_user_tag\"" +
                    " (" +
                    "\"userid\"," +
                    "\"tagcount\"," +
                    "\"tagcode\"," +
                    "\"flow\"," +
                    "\"refer\"," +
                    "\"createtime\"," +
                    "\"duration\")"
            );
            sql.append(" VALUES (")
                    .append("\'")
                    .append(msisdn)
                    .append("\',\'")
                    .append(tagLevel)
                    .append("\',\'")
                    .append(tagCode)
                    .append("\',\'")
                    .append(visitOctets)
                    .append("\',\'")
                    .append(host)
                    .append("\',\'")
                    .append(time)
                    .append("\',\'")
                    .append(duration)
                    .append("\'")
                    .append(");");

            context.write(NullWritable.get(),new Text(sql.toString()));
        }
    }

}
