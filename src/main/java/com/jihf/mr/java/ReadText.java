package com.jihf.mr.java;

import com.jihf.mr.utils.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import javax.xml.transform.Source;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-25 18:02
 * Mail：jihaifeng@raiyi.com
 */
public class ReadText {

    public static void main(String[] args) {
        File file = new File("E://excel/hostResult1.txt");
        txt2String(file);
        ;
    }

    /**
     * 读取txt文件的内容
     *
     * @param file 想要读取的文件对象
     * @return 返回文件内容
     */
    private static String txt2String(File file) {
        int count = 0;
        StringBuilder result = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            int num = 0;
            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                String[] datas = s.split("\\|", -1);
                 num += Integer.parseInt(datas[5]);


            }
            System.out.println(num);
            br.close();
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }

        return result.toString();
    }
}
