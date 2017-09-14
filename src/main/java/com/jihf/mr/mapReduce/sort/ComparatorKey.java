package com.jihf.mr.mapReduce.sort;

import java.util.Comparator;

//自定义比较器方法
public class ComparatorKey implements Comparator {

    @Override
    public int compare(Object arg0, Object arg1) {
        VisitInfo user0 = (VisitInfo) arg0;
        VisitInfo user1 = (VisitInfo) arg1;
        //比较key
        int result = user0.getKey().compareTo(user1.getKey());
        //加了负号，表示降序输出
        return -result;
    }
}