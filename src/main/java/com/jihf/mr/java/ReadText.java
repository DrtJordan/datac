package com.jihf.mr.java;

import com.jihf.mr.utils.*;
import model.DpiResult;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.*;
import java.text.ParseException;
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
        File file = new File("E://excel/host1000.txt");
        txt2String(file);
//        iteratorAddFiles("jihaifeng/testComplain/{20170824,20170909}/32");

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
  StringBuffer sb_ua = new StringBuffer("");
        try {

            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while ((s = br.readLine()) != null) {
                //使用readLine方法，一次读一行
                String[] datas = s.toString().split("\\|", -1);
                String ua = datas[1];
                if (StringUtils.strIsNotEmpty(ua)) {
                    if (!ua.toLowerCase().contains("weixi") && !ua.toLowerCase().contains("qq.com")) {
                        sb_ua.append(ua+"\n");
                    }
//                    classifyUa(ua);
                }


//                DatumReader<DpiResult> reader = new SpecificDatumReader<DpiResult>(DpiResult.class);
//                DataFileReader<DpiResult> dataFileReader = new DataFileReader<DpiResult>(file, reader);
//                FileWriter fw = new FileWriter("E://excel/20170909/phoneNum_20170909.txt");
//                BufferedWriter bw = new BufferedWriter(fw);
//                for (int i = 0; i < 1000; i++) {
//                    while (dataFileReader.hasNext()) {
//                        DpiResult dpiResult = dataFileReader.next();
//                        if (!StringUtils.strIsEmpty(dpiResult.getPhoneNumber().toString())) {
//                            bw.write(dpiResult.getPhoneNumber().toString() + "\n");
//                        }
//                    }
//                    bw.close();
//                    fw.close();


            }
            System.out.println(count);
            writeUa("E://excel/host1000_1.txt",sb_ua.toString());

//            writeUa(path_iphone, sb_iphone.toString());
//            writeUa(path_vivo, sb_vivo.toString());
//            writeUa(path_xiaomi, sb_xiaomi.toString());
//            writeUa(path_huawei, sb_huawei.toString());
//            writeUa(path_oppo, sb_oppo.toString());
//            writeUa(path_nexus, sb_nexus.toString());
//            writeUa(path_le, sb_le.toString());
//            writeUa(path_sanxing, sb_sanxing.toString());
//            writeUa(path_mc, sb_mc.toString());
//            writeUa(path_aMap, sb_aMap.toString());
//            writeUa(path_other, sb_other.toString());
//
//            System.out.println("iphone:" + iphoneCount);
//            System.out.println("vivo:" + vivoCount);
//            System.out.println("xiaomi:" + xiaomiCount);
//            System.out.println("huawei:" + huaweiCount);
//            System.out.println("oppo:" + oppoCount);
//            System.out.println("nexus:" + nexusCount);
//            System.out.println("leshi:" + leCount);
//            System.out.println("sanxing:" + sanxingCount);
//            System.out.println("mc:" + mcCount);
//            System.out.println("aMap:" + aMapCount);
//            System.out.println("other:" + otherCount);


            br.close();
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }

        return result;
    }


    private static int iphoneCount = 0;
    private static String path_iphone = "E://excel/phone/nexus.txt";
    private static StringBuffer sb_iphone = new StringBuffer("");

    private static int vivoCount = 0;
    private static String path_vivo = "E://excel/phone/vivo.txt";
    private static StringBuffer sb_vivo = new StringBuffer("");

    private static int xiaomiCount = 0;
    private static String path_xiaomi = "E://excel/phone/xiaomi.txt";
    private static StringBuffer sb_xiaomi = new StringBuffer("");

    private static int huaweiCount = 0;
    private static String path_huawei = "E://excel/phone/huawei.txt";
    private static StringBuffer sb_huawei = new StringBuffer("");

    private static int oppoCount = 0;
    private static String path_oppo = "E://excel/phone/oppo.txt";
    private static StringBuffer sb_oppo = new StringBuffer("");

    private static int nexusCount = 0;
    private static String path_nexus = "E://excel/phone/nexus.txt";
    private static StringBuffer sb_nexus = new StringBuffer("");

    private static int leCount = 0;
    private static String path_le = "E://excel/phone/le.txt";
    private static StringBuffer sb_le = new StringBuffer("");

    private static int sanxingCount = 0;
    private static String path_sanxing = "E://excel/phone/sanxing.txt";
    private static StringBuffer sb_sanxing = new StringBuffer("");

    private static int otherCount = 0;
    private static String path_other = "E://excel/phone/other.txt";
    private static StringBuffer sb_other = new StringBuffer("");


    private static int mcCount = 0;
    private static String path_mc = "E://excel/phone/mc.txt";
    private static StringBuffer sb_mc = new StringBuffer("");

    private static int aMapCount = 0;
    private static String path_aMap = "E://excel/phone/aMap.txt";
    private static StringBuffer sb_aMap = new StringBuffer("");

    private static void classifyUa(String ua) {
        if (ua.toLowerCase().contains("vivo")) {
            vivoCount++;
            sb_vivo.append(ua + "\n");
        } else if (ua.toLowerCase().contains("redmi") || ua.toLowerCase().contains("xiaomi") || ua.toLowerCase().contains("miui")) {
            xiaomiCount++;
            sb_xiaomi.append(ua + "\n");
        } else if (ua.toLowerCase().contains("huawei") || ua.toLowerCase().contains("honor")) {
            huaweiCount++;
            sb_huawei.append(ua + "\n");
        } else if (ua.toLowerCase().contains("oppo")) {
            oppoCount++;
            sb_oppo.append(ua + "\n");
        } else if (ua.toLowerCase().contains("nexus")) {
            nexusCount++;
            sb_nexus.append(ua + "\n");
        } else if (ua.toLowerCase().contains("iphone")) {
            iphoneCount++;
            sb_iphone.append(ua + "\n");
        } else if (ua.toLowerCase().contains("le")) {
            leCount++;
            sb_le.append(ua + "\n");
        } else if (ua.toLowerCase().contains("sm")) {
            sanxingCount++;
            sb_sanxing.append(ua + "\n");
        } else if (ua.toLowerCase().contains("micromessenger client")) {
            mcCount++;
            sb_mc.append(ua + "\n");
        } else if (ua.toLowerCase().contains("amap_location_sdk_android")) {
            aMapCount++;
            sb_aMap.append(ua + "\n");
        } else {
            otherCount++;
            sb_other.append(ua + "\n");
        }

    }

    private static void writeUa(String filePath, String data) {
        try {
            FileWriter fw = new FileWriter(filePath);
            BufferedWriter bw = new BufferedWriter(fw);

            bw.write(data);

            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


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
