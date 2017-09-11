package com.jihf.mr.utils;

import org.apache.hadoop.conf.Configuration;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-21 09:29
 * Mail：jihaifeng@raiyi.com
 */
public class MrUtils {

    public static Configuration getConFiguration(String qName){
        Configuration cf = new Configuration();
        cf.set("mapreduce.map.failures.maxpercent", "90");
        cf.set("mapreduce.reduce.failures.maxpercent", "90");

        String queueName = qName;
        cf.set("mapreduce.job.queuename", queueName.trim());
        return cf;
    }
    public static Configuration getRaiyiConfiguration(){
        return getConFiguration("root.vendor.ven27");
    }
}
