package com.jihf.mr.utils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-05 15:10
 * Mail：jihaifeng@raiyi.com
 */
public class UrlHandler {
    static List<String> mblNumList = new ArrayList<String>();


    public static void main(String[] args) {
        init();
        System.out.println(mblNumList.size());
    }

    private static void init() {
        if (null != mblNumList && mblNumList.size() != 0){
            return;
        }
        try {
            InputStream in = UrlHandler.class.getClassLoader().getResourceAsStream("mblNumKey.txt");
            InputStreamReader isr = new InputStreamReader(in);
            BufferedReader br = new BufferedReader(isr);
            String lineNext = null;
            while ((lineNext = br.readLine()) != null) {
                if (!StringUtils.strIsEmpty(lineNext)) {
                    String temp = lineNext.substring(1, lineNext.length() - 1);
                    if (!StringUtils.strIsEmpty(temp) && temp.contains(",")) {
                        String key = temp.split(",")[0];
                        if (!mblNumList.contains(key)) {
                            mblNumList.add(key);
                        }
                    } else {
                        System.err.println("temp：" + temp);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String matchMblNumKey(String str) {
        init();
        String matchKey = null;
        if (mblNumList.size() != 0) {
            for (String key : mblNumList) {
                if (key.equals(str)) {
                    matchKey = key;
                }
            }
        }
        return matchKey;
    }


    /**
     * 解析出url请求的路径，包括页面
     *
     * @param strURL url地址
     * @return url路径
     */
    public static String UrlPage(String strURL) {
        String strPage = null;
        String[] arrSplit = null;

        strURL = strURL.trim().toLowerCase();

        arrSplit = strURL.split("[?]");
        if (strURL.length() > 0) {
            if (arrSplit.length > 1) {
                if (arrSplit[0] != null) {
                    strPage = arrSplit[0];
                }
            }
        }

        return strPage;
    }

    /**
     * 解析出url请求的主機名Host
     *
     * @param strURL url地址
     * @return host
     */
    public static String getHost(String strURL) {
        if (StringUtils.strIsEmpty(strURL))
            return null;
        try {
            URL url = new URL(strURL);
            return getHost(url);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 解析出url请求的主機名Host
     *
     * @param url url地址
     * @return host
     */
    public static String getHost(URL url) {
        if (StringUtils.strIsEmpty(url.toString())) {
            return null;
        }
        String host = url.getHost();
        return host;
    }


    /**
     * 去掉url中的路径，留下请求参数部分
     *
     * @param strURL url地址
     * @return url请求参数部分
     */
    private static String TruncateUrlPage(String strURL) {
        if (StringUtils.strIsEmpty(strURL)) {
            return null;
        }
        String strAllParam = null;
        String[] arrSplit = null;

        strURL = strURL.trim().toLowerCase();

        arrSplit = strURL.split("[?]");
        if (strURL.length() > 1) {
            if (arrSplit.length > 1) {
                if (arrSplit[1] != null) {
                    strAllParam = arrSplit[1];
                }
            }
        }

        return strAllParam;
    }

    /**
     * 解析出url参数中的键值对
     * 如 "index.jsp?Action=del&id=123"，解析出Action:del,id:123存入map中
     *
     * @param URL url地址
     * @return url请求参数的Map鍵值對
     */
    public static Map<String, String> URLRequest(String URL) {
        Map<String, String> mapRequest = new HashMap<String, String>();

        String[] arrSplit = null;

        String strUrlParam = TruncateUrlPage(URL);
        if (strUrlParam == null) {
            return mapRequest;
        }
        //每个键值为一组
        arrSplit = strUrlParam.split("[&]");
        for (String strSplit : arrSplit) {
            String[] arrSplitEqual = null;
            arrSplitEqual = strSplit.split("[=]");

            //解析出键值
            if (arrSplitEqual.length > 1) {
                //正确解析
                mapRequest.put(arrSplitEqual[0], arrSplitEqual[1]);

            } else {
                if (!StringUtils.strIsEmpty(arrSplitEqual) && !arrSplitEqual[0].equals("")) {
                    //只有参数没有值，不加入
                    mapRequest.put(arrSplitEqual[0], "");
                }
            }
        }
        return mapRequest;
    }
}
