package com.jihf.mr.java;

import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataBean;
import com.jihf.mr.mapReduce.hiveFflowData.HiveFlowDataUtils;
import com.jihf.mr.utils.HDFSFileUtils;
import com.jihf.mr.utils.Matcher;
import com.jihf.mr.utils.StringUtils;
import com.raiyi.dpiModel.DpiResult;
import com.raiyi.dpiModel.HostPair;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
        File file = new File("E://excel/20170909/jiangsu_dpi_0909.avro");
        txt2String(file);
//        try {
//            long dateTime = new SimpleDateFormat("yyyyMMdd").parse("20170804").getTime();
//        } catch (ParseException e) {
//            System.out.println(e);
//            e.printStackTrace();
//        }
    }

    private static boolean matchChe(String url) {
        Matcher matcher = new Matcher(true);
        matcher.addPattern("*jbzs.12321.cn*", 100);
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
                //使用readLine方法，一次读一行
//                String[] datas = s.toString().split("\u0001", -1);
//                HiveFlowDataBean flowDataBean = HiveFlowDataUtils.parse2FlowBean(datas);
//                if (null != flowDataBean && !StringUtils.strIsEmpty(flowDataBean.flow_total) && !StringUtils.strIsEmpty(flowDataBean.flow_used) && !StringUtils.strIsEmpty(flowDataBean.main_price)) {
//                    if (Double.parseDouble(flowDataBean.main_price) > 0) {
//                        System.out.println(flowDataBean.main_price);
//                    }
//                }

                DatumReader<DpiResult> reader = new SpecificDatumReader<DpiResult>(DpiResult.class);
                DataFileReader<DpiResult> dataFileReader = new DataFileReader<DpiResult>(file, reader);
                FileWriter fw = new FileWriter("E://excel/20170909/phoneNum_20170909.txt");
                BufferedWriter bw = new BufferedWriter(fw);
                for (int i = 0; i < 1000; i++) {
                    while (dataFileReader.hasNext()) {
                        DpiResult dpiResult = dataFileReader.next();
//                    System.out.println(dpiResult.getPhoneNumber());
                        if (!StringUtils.strIsEmpty(dpiResult.getPhoneNumber().toString())) {
                            bw.write(dpiResult.getPhoneNumber().toString()+"\n");
                        }
                    }
                    bw.close();
                    fw.close();


//                    List<HostPair> hostPairList = dpiResult.getHostFreq();
//                    for (HostPair hostPair : hostPairList) {
//                        if (matchChe(hostPair.getHost().toString().toLowerCase())){
//                            System.out.println(String.format("%s",hostPair.getHost(),hostPair.getFrequency()));
//                        }
//
//                    }


                }
            }
//            for (String url : urlList) {
//                System.out.println("url：" + url);
//            }

            br.close();
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }

        return result;
    }


    private static boolean isUp0(long... num) {
        boolean flag = true;
        for (long n : num) {
            if (n <= 0) {
                flag = false;
            }
        }
        return flag;
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
