package com.jihf.mr.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-11 09:11
 * Mail：jihaifeng@raiyi.com
 */
public class HDFSFileUtils {

    // 输入文件去重
    public static boolean isContain(Job job, String str) {
        Path[] paths = FileInputFormat.getInputPaths(job);
        boolean flag = false;
        for (Path p : paths) {
            if (p.toUri().getPath().equals(new Path(str).toString())) {
                flag = true;
            }
        }
        return flag;
    }

    public static void delPathIfExits(Configuration cf, Path path) {
        try {
            FileSystem fs = FileSystem.get(cf);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isFile(Configuration cf, Path path) {
        try {
            FileSystem fs = FileSystem.get(cf);
            return fs.exists(path) && fs.isFile(path);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static Path getPath(Configuration cf, String p) {
        if (StringUtils.strIsEmpty(p)) {
            throw new NullPointerException("path  is empty.");
        }
        Path _path = new Path(p);
        delPathIfExits(cf, _path);
        return _path;
    }

    /**
     * 投诉模型输入目录格式
     * <p>
     * input:
     * 1、单独文件（夹）或单个文件（夹）：
     * jihaifeng/testComplain/jiangsu_0904.avro
     * jihaifeng/testComplain/
     * jihaifeng/testComplain/jiangsu_0904.avro&jihaifeng/testComplain/jiangsu_0905
     * jihaifeng/testComplain/&jihaifeng/testComplain/
     * 2、多个文件夹：
     * jihaifeng/testComplain/{20170904,20170909}/32
     *
     * @param cf
     * @param job
     * @param input
     * @param inputFormatClass
     * @param mapperClass
     */
    public static void initInputPath(Configuration cf, Job job, String input, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass) {
        if (input.contains("{")) {
             // 处理日期区间
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String pathStr = null;
            // 需要处理输入区间
            if (!input.contains("}")) {
                JobUtils.exit("input is illegal. please input like <jihaifeng/testComplain/{20170904,20170909}/32>");
            }
            int startIndex = input.indexOf("{");
            int endIndex = input.indexOf("}");
            String _temp = input.substring(startIndex, endIndex + 1);
            String[] params = _temp.substring(1, _temp.length() - 1).split(",", -1);
            if (params.length != 2) {
                System.err.println("params is illegal. please input like <jihaifeng/testComplain/{20170904,20170909}/32>");
                return;
            }
            String startDate = params[0];
            String endDate = params[1];
            String _tempDate = startDate;
            System.out.println(pathStr);
            try {
                while (sdf.parse(_tempDate).before(sdf.parse(endDate)) || _tempDate.equals(endDate)) {
                    pathStr = input.replace(_temp, _tempDate);
                    iteratorAddFiles(cf, job, new Path(pathStr), inputFormatClass, mapperClass);
                    _tempDate = TimeUtils.checkOption("next", _tempDate);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        } else {
            /**
             * 单独文件（夹）或单个文件（夹）：
             *  jihaifeng/testComplain/jiangsu_0904.avro
             *  jihaifeng/testComplain/
             *  jihaifeng/testComplain/jiangsu_0904.avro&jihaifeng/testComplain/jiangsu_0905
             *  jihaifeng/testComplain/&jihaifeng/testComplain/
             */
            String[] inputs = input.split("&", -1);
            for (String in : inputs) {
                Path path = new Path(in);
                iteratorAddFiles(cf, job, path, inputFormatClass, mapperClass);
            }
        }
    }

    private static void iteratorAddFiles(Configuration conf, Job job, Path path, Class<? extends InputFormat> inputFormatClass, Class<? extends Mapper> mapperClass) {
        if (isObjectNull(conf, job, path, inputFormatClass, mapperClass)) {
            JobUtils.exit("failure to addInputPath");
        }
        try {
            FileSystem hdfs = FileSystem.get(conf);
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);

            //展示文件信息
            for (FileStatus file : files) {
                try {
                    if (file.isDirectory()) {
                        //递归调用
                        iteratorAddFiles(conf, job, file.getPath(), inputFormatClass, mapperClass);
                    } else if (file.isFile() && !file.getPath().getName().equals("_SUCCESS")) {
                        MultipleInputs.addInputPath(job, file.getPath(), inputFormatClass, mapperClass);
                    }
                } catch (Exception e) {
                    JobUtils.exit("iteratorAddFiles e：" + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            JobUtils.exit("e：" + e.getMessage());
            e.printStackTrace();
        }
    }

    private static boolean isObjectNull(Object... objets) {
        boolean flag = false;
        for (Object obj : objets) {
            if (null == obj) {
                flag = true;
            }
        }
        return flag;
    }
}
