package com.jihf.mr.java;

import com.jihf.mr.utils.Matcher;

import java.io.*;
import java.util.*;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-16 11:41
 * Mail：jihaifeng@raiyi.com
 */
public class FileCompare {
    private static List<String> list = new ArrayList<String>();
    private static List<String> list1 = new ArrayList<String>();
    private static List<String> list2 = new ArrayList<String>();

    public static void main(String[] args) throws IOException {
        FileCompare c = new FileCompare();
        c.compareFile();

        Matcher matcher = new Matcher(true);
    }

    public void compareFile() {
        File file = new File("E://excel/样本/样本号码/样本投诉号码.txt");
//        File file = new File("E://excel/fixMsisdn.txt");
        File file2 = new File("E://excel/样本/样本号码/样本回复号码.txt");
        Set fileTextSet = new HashSet();
        Set file2TextSet = new HashSet();
        try {
            fileTextSet = getText(file);
            file2TextSet = getText(file2);
            compareSet(fileTextSet, file2TextSet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void compareSet(Set textSet, Set StextSet2) {

        for (Iterator iterator = textSet.iterator(); iterator.hasNext(); ) {
            String name = (String) iterator.next();
            if (StextSet2.contains(name)) {
                list.add(name);
//                System.out.println("共同报文：" + name);
            } else {
                list1.add(name);
//                System.out.println("1独有报文：" + name);
            }
        }
        for (Iterator iterator = StextSet2.iterator(); iterator.hasNext(); ) {
            String name = (String) iterator.next();
            if (!textSet.contains(name)) {
                list2.add(name);
//                System.out.println("2独有报文：" + name);
            }
        }
        System.out.println("共同报文长度：" + list.size()
                + "\n1报文长度：" + textSet.size()
                + "\n2报文长度：" + StextSet2.size());
    }

    private Set getText(File file) throws IOException {
        Set strSet = new HashSet();
        BufferedReader br = null;
        InputStream is = null;
        try {
            is = new FileInputStream(file);
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String lineStr = null;
            while ((lineStr = br.readLine()) != null) {
//                String text = lineStr.substring(lineStr.indexOf(":"));//按照需求切分
                strSet.add(lineStr);
            }
            br.close();
            is.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                br.close();
            }
            if (is != null) {
                is.close();
            }
        }
        return strSet;
    }
}