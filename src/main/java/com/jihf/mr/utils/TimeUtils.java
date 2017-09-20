package com.jihf.mr.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-16 15:02
 * Mail：jihaifeng@raiyi.com
 */
public class TimeUtils {
    /*
    * 将时间转换为时间戳
    */
    public static Long dateToStamp(String s) {
        long ts = 0;
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            Date date = simpleDateFormat.parse(s);
            ts = date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return ts;
    }

    public static String formatTime(String s) {
        String time = null;
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = simpleDateFormat.parse(s);
            time = sdf.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }

    /*
     * 将时间戳转换为时间
     */
    public static String stampToDate(String s) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

    /***
     * 日期减一天、加一天
     *
     * @param option
     *            传入类型 pro：日期减一天，next：日期加一天
     * @param _date
     *            20141124
     * @return 减一天：20141123或(加一天：20141125)
     */
    public static String checkOption(String option, String _date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar cl = Calendar.getInstance();
        Date date = null;

        try {
            date = (Date) sdf.parse(_date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        cl.setTime(date);
        if ("pre".equals(option)) {
            // 时间减一天
            cl.add(Calendar.DAY_OF_MONTH, -1);

        } else if ("next".equals(option)) {
            // 时间加一天
            cl.add(Calendar.DAY_OF_YEAR, 1);
        } else {
            // do nothing
        }
        date = cl.getTime();
        return sdf.format(date);
    }
}
