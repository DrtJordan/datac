package com.jihf.mr.java;


import com.jihf.mr.utils.MD5Utils;
import com.jihf.mr.utils.StringUtils;
import jxl.Workbook;
import jxl.read.biff.BiffException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-16 11:24
 * Mail：jihaifeng@raiyi.com
 */
public class ReadExcelCls {
    public static List<String> list = new ArrayList<String>();
    public static final boolean duplicateRemoval = true;
    public static final int sheetNum = 0; // excel底部sheet标签位置 ，从0开始
    public static final int startRow = 2; // 需要读取的开始行 ，从1开始
    public static final int endRow = 2632; // 需要读取的结束行，从1开始
    public static final int startCol = 2; // 需要读取的开始列，从1开始
    public static final int endCol = 2; // 需要读取的结束列 ，从1开始

    public static void main(String[] args) throws BiffException, IOException {

//        readCol();
        System.out.println("21a313123".matches("\\d+"));
    }

    private static long getScore(String callType, int count, String callDuration) {
        if (StringUtils.strIsEmpty(callDuration) || !callDuration.matches("\\d+")) {
            return -1;
        }
        long score = 0;
        int y = 20;
        int z = 10;
        int B = count;

        long C = Long.parseLong(callDuration);
        score = (B - 3) * y + (C - 120) / 10 * z;
        return score;
    }

    private static void readCol() {
        try {
            FileOutputStream fs = new FileOutputStream(new File("E://excel/6-7月phoneNum.txt"));
            PrintStream p = new PrintStream(fs);

            FileOutputStream fs1 = new FileOutputStream(new File("E://excel/6-7月phoneMD5.txt"));
            PrintStream p1 = new PrintStream(fs1);
            //生成File实例并指向需要读取的Excel表文件
            File file = null;
            file = new File("E://excel/样本/6-7月投诉清单.xls");

            Workbook wb = ExcelReader.getWorkBook(file);
            //读取Excel文件标签索引为0的表中的第1行，从第1行读到第6行结束
            String[] data = ExcelReader.readExcelRowOrCol(wb, 0,
                    2, 15551,
                    8, 8);
            for (int i = 0; i < data.length; i++) {


                if (!list.contains(data[i])) {
                    p.println(data[i]);
                    list.add(data[i]);
                    p1.println(MD5Utils.EncoderByMd5(data[i]));
                }

            }
            System.out.print(list.size());
            p1.close();
            p.close();
            System.out.println();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
