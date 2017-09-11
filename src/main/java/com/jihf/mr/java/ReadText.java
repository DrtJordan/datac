package com.jihf.mr.java;

import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataBean;
import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataUtils;
import com.jihf.mr.utils.Matcher;
import com.jihf.mr.utils.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import javax.xml.transform.Source;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URLStreamHandler;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-25 18:02
 * Mail：jihaifeng@raiyi.com
 */
public class ReadText {
    static List<String> urlList = new ArrayList<String>();

    public static void main(String[] args) {
        File file = new File("E://excel/002192_0");
        txt2String(file);
    }

    private static boolean matchChe(String url) {
        Matcher matcher = new Matcher(true);
        matcher.addPattern("*.guazi.com/*/sell*", 100);
        matcher.addPattern("*.guazi.com/*/sale*", 200);
        if (StringUtils.strIsEmpty(url)) {
            return false;
        }
        int index = -1;
        String _tempUrl = url;
//        if (url.startsWith("http://")) {
//            index = 7;
//        } else if (url.startsWith("https://")) {
//            index = 8;
//        }
//        if (index != -1) {
//            _tempUrl = url.substring(index, url.length());
//        }
        if (!StringUtils.strIsEmpty(_tempUrl)) {
            Matcher.MatchResult[] a = matcher.match(_tempUrl);
            if (a.length != 0) {
                System.out.println(url);
                return true;
            }
        }
        return false;
    }

    /**
     * 读取txt文件的内容
     *
     * @param file 想要读取的文件对象
     * @return 返回文件内容
     */
    private static String txt2String(File file) {

        List<String> ll = new ArrayList<String>();
        ll.add("17526785002");
        ll.add("17526786009");
        ll.add("17526792838");
        ll.add("17526795488");
        int count = 0;
        String result = null;
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while ((s = br.readLine()) != null) {
//                //使用readLine方法，一次读一行
                String[] datas = s.split("\u0001", -1);
                HiveFlowDataBean flowDataBean = HiveFlowDataUtils.parse2FlowBean(datas);
                int day =  Integer.parseInt(flowDataBean.date.substring(8,flowDataBean.date.length()));
                if (ll.contains(flowDataBean.mobile)) {
                    System.out.println(String.format("%s|%s|%s|%s|%s|%s",
                            flowDataBean.mobile,
                            flowDataBean.flow_total,
                            flowDataBean.flow_over,
                            flowDataBean.flow_used,
                            flowDataBean.main_price,
                            day));
                }


            }
//            System.out.println(urlList.size());
//            urlList = filterUrlList();
//            System.out.println(urlList.size());
            for (String url : urlList) {
//                openPage(url);
                System.out.println("url：" + url);
            }

            br.close();
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }

        return result;
    }

    private static List<String> filterUrlList() {
        Matcher matcher = new Matcher(true);
        if (null == urlList || urlList.size() == 0) {
            return urlList;
        }
        List<String> tempUrlList = new ArrayList<String>();
        for (String url : urlList) {
            int index = -1;
            String _tempUrl = null;
            if (url.startsWith("http://")) {
                index = 7;
            } else if (url.startsWith("https://")) {
                index = 8;
            }
            if (index != -1) {
                _tempUrl = url.substring(index, url.length());
            }
            matcher.addPattern("sempage.guazi.com/*/sell*", 100);
            matcher.addPattern("m.guazi.com/*/sell*", 200);
            matcher.addPattern("sta.guazi.com/we_client*", 300);
            if (!StringUtils.strIsEmpty(_tempUrl)) {
                Matcher.MatchResult[] a = matcher.match(_tempUrl);
                if (a.length != 0) {
                    tempUrlList.add(url);
                }
            }
        }
        System.out.println("tempUrlList：" + tempUrlList.size());
        return tempUrlList;
    }

    public static void openPage(String url) {
        try {
            java.net.URI uri = java.net.URI.create(url);
            // 获取当前系统桌面扩展
            java.awt.Desktop dp = java.awt.Desktop.getDesktop();
            // 判断系统桌面是否支持要执行的功能
            if (dp.isSupported(java.awt.Desktop.Action.BROWSE)) {
                //File file = new File("D:\\aa.txt");
                //dp.edit(file);// 　编辑文件
                dp.browse(uri);// 获取系统默认浏览器打开链接
                // dp.open(file);// 用默认方式打开文件
                // dp.print(file);// 用打印机打印文件
            }
        } catch (java.lang.NullPointerException e) {
            // 此为uri为空时抛出异常
            e.printStackTrace();
        } catch (java.io.IOException e) {
            // 此为无法获取系统默认浏览器
            e.printStackTrace();
        }
    }

    private static void addUrl(String url) {
        if (null != url && !urlList.contains(url)) {
            urlList.add(url);

        }
        System.out.println("size：" + urlList.size());
//        if (null != url
//                && !url.endsWith(".png")
//                && !url.endsWith(".jpg")
//                && !url.endsWith(".js")
//                && !url.startsWith("http://subjunction58.m.guazi.com")
//                && !url.startsWith("http://bzclk.baidu.com")
//                && !url.startsWith("http://analytics.guazi.com")
//                && !url.startsWith("http://p.gif")
//                && !url.startsWith("http://image.guazistatic.com")
//                && !url.startsWith("http://t.guazi.com/t.gif")
//                && !url.startsWith("http://hm.baidu.com/hm.gif")
//                && !url.startsWith("http://static.58.com")
//                && !url.startsWith("http://m.58.com")
//                && !urlList.contains(url)) {
//            urlList.add(url);
//        }
    }
}
