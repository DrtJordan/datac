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
        File file = new File("E://excel/result.txt");
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
            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                String[] datas = s.split("\\|", -1);
                String hostStr = datas[1];
                if (!StringUtils.strIsEmpty(hostStr)) {
                    if (!hostStr.matches("\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}")
                            && !hostStr.contains("qq.com")
                            && !hostStr.contains("qpic.cn")
                            && !hostStr.contains("weibo.cn")
                            && !hostStr.contains("alipay")
                            && !hostStr.contains("qzone")
                            && !hostStr.contains("sina")
                            && !hostStr.contains("letv.com")
                            && !hostStr.contains("baidu.com")
                            && !hostStr.contains("moji.com")
                            && !hostStr.contains("souhu.com")
                            && !hostStr.contains("wandoujia.com")
                            && !hostStr.contains("kugou.com")
                            && !hostStr.contains("meizu.com")
                            && !hostStr.contains("flyme.cn")) {
//                        System.out.println("hostStr：" + hostStr);
                    }
                    if (hostStr.contains("189")){

                        System.out.println("err：" + hostStr);
                    }
                }
            }
            br.close();
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }

        return result.toString();
    }
}
