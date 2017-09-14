package com.jihf.mr.mapReduce;

import com.jihf.mr.constants.Config;
import com.jihf.mr.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-11 14:12
 * Mail：jihaifeng@raiyi.com
 */
public class MatchDpiHostWithChe {
   private static Matcher matcher = new Matcher(true);

    public static class fixDPiMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fixData = value.toString();
            String imeiStr = null;
            String imsiStr = null;

            String[] datas = fixData.split("\005");
            String UserAccount = StringUtils.strIsEmpty(datas[0]) ? null : datas[0];
            String Protocol = StringUtils.strIsEmpty(datas[1]) ? null : datas[1];
            String SourceIP = StringUtils.strIsEmpty(datas[2]) ? null : datas[2];
            String DestinationIP = StringUtils.strIsEmpty(datas[3]) ? null : datas[3];
            String SourcePort = StringUtils.strIsEmpty(datas[4]) ? null : datas[4];
            String DestinationPort = StringUtils.strIsEmpty(datas[5]) ? null : datas[5];
            String DomainName = StringUtils.strIsEmpty(datas[6]) ? null : datas[6];
            String URL = StringUtils.strIsEmpty(datas[7]) ? null : datas[7];
            String REFERER = StringUtils.strIsEmpty(datas[8]) ? null : datas[8];
            String UserAgent = StringUtils.strIsEmpty(datas[9]) ? null : datas[9];
            String Cookie = StringUtils.strIsEmpty(datas[10]) ? null : datas[10];
            String accessTime = StringUtils.strIsEmpty(datas[11]) ? null : datas[11];

            context.write(new Text(String.format("%s|%s", URL, UserAgent)), new IntWritable(1));

        }
    }

    public static class mblDPiMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fixData = value.toString();

            String[] datas = fixData.split("\\|", -1);
            if (datas.length > 38) {
                String imsi = StringUtils.strIsEmpty(datas[0]) ? null : datas[0];
                String msisdn = StringUtils.strIsEmpty(datas[1]) ? null : datas[1];
                String imei = StringUtils.strIsEmpty(datas[2]) ? null : datas[2];

                String UserAgent = StringUtils.strIsEmpty(datas[28]) ? null : datas[28];
                String URL = StringUtils.strIsEmpty(datas[29]) ? null : datas[29];
                String doMain = StringUtils.strIsEmpty(datas[30]) ? null : datas[30];
                String host = StringUtils.strIsEmpty(datas[31]) ? null : datas[31];
                String refer = StringUtils.strIsEmpty(datas[35]) ? null : datas[35];

                context.write(new Text(String.format("%s|%s|%s|%s", URL, UserAgent, msisdn, refer)), new IntWritable(1));

            }
        }
    }

    public static class hostCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           initMatcher();
        }



        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 取手机号
            String[] datas = key.toString().split("\\|", -1);
            String url = null;
            String ua = null;
            String phone = null;

            if (datas.length == 2) {
                url = datas[0];
                ua = datas[1];
                Map<String, String> paramsMap = UrlHandler.URLRequest(url);
                if (null != paramsMap && paramsMap.size() != 0) {
                    String phonekey = null;
                    for (String mapKey : paramsMap.keySet()) {
                        if (!StringUtils.strIsEmpty(mapKey) && !StringUtils.strIsEmpty(UrlHandler.matchMblNumKey(mapKey))) {
                            phonekey = UrlHandler.matchMblNumKey(mapKey);
                        }
                    }
                    if (!StringUtils.strIsEmpty(phonekey)) {
                        phone = StringUtils.getTelnum(paramsMap.get(phonekey));
                    }
                }
            } else if (datas.length == 4) {
                ua = datas[1];
                phone = datas[2];
                if (matchChe(datas[0])) {
                    url = datas[0];
                } else if (matchChe(datas[3])) {
                    url = datas[3];
                }
            }
            if (!StringUtils.strIsEmpty(url) && !StringUtils.strIsEmpty(phone)) {
                context.write(new Text(String.format("%s|%s|%s", phone, url, ua)), new IntWritable(1));
            }

        }

        private boolean matchChe(String url) {
            if (StringUtils.strIsEmpty(url)) {
                return false;
            }
            int index = -1;
            String _tempUrl = url;
            if (!StringUtils.strIsEmpty(_tempUrl)) {
                Matcher.MatchResult[] a = matcher.match(_tempUrl);
                if (a.length != 0) {
                    return true;
                }
            }
            return false;
        }

    }

    public static class hostReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable i : values) {
                count++;
            }
            context.write(new Text(key), NullWritable.get());
        }

    }

    public static void main(String[] args) {

        try {

            String input1 = Config.MOBILE_DPI_INPUT;
            String output = Config.MOBILE_DPI_OUTPUT;
            String inputType = "1";
            String dataType = "1";
            if (null != args && args.length != 0) {
                if (args.length == 4) {
                    inputType = args[0];
                    dataType = args[1];
                    input1 = args[2];
                    output = args[3];
                } else {
                    JobUtils.exit("the num of parameter is illegal.");
                }
            }
            if (inputType.endsWith("-1")) {
                JobUtils.exit("please set the type of input.<1  SequenceFileInputFormat> <2 TextInputFormat>");
            }
            Configuration conf = MrUtils.getRaiyiConfiguration();

            Job job = Job.getInstance(conf, "fixData");
            job.setJarByClass(MatchDpiHostWithChe.class);

            if (dataType.equals("1")) {
                job.setMapperClass(mblDPiMapper.class);
            } else if (dataType.equals("2")) {
                job.setMapperClass(fixDPiMapper.class);
            } else {
                JobUtils.exit("please set the type of input data.<1 MobileData> <2 FixData>");
            }
            job.setCombinerClass(hostCombiner.class);
            job.setReducerClass(hostReducer.class);

            job.setMapOutputValueClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);


            if (inputType.equals("1")) {
                job.setInputFormatClass(SequenceFileInputFormat.class);
            } else if (inputType.equals("2")) {
                job.setInputFormatClass(TextInputFormat.class);
            } else {
                JobUtils.exit("please set the correct type of input data.<1  SequenceFileInputFormat> <2 TextInputFormat>");
            }
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(input1));
            FileOutputFormat.setOutputPath(job, HDFSFileUtils.getPath(conf, output));

            System.out.println("\n==================================\n");
            System.out.println("输入目录：" + input1);
            System.out.println("输出目录：" + output);
            System.out.println("\n==================================\n");

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            JobUtils.exit(e.getMessage());
            e.printStackTrace();
        }
    }
    private  static void initMatcher() {
        matcher.addPattern("*.guazi.com/*/sell*", 100);
        matcher.addPattern("*.guazi.com/*/sale*", 200);
//            matcher.addPattern("sta.guazi.com/we_client*", 300);
    }
}
