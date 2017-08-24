package com.jihf.mr.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

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
                fs.delete(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
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
}
