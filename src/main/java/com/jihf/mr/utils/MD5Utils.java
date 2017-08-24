package com.jihf.mr.utils;

import java.security.MessageDigest;
import java.text.ParseException;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-11 09:48
 * Mail：jihaifeng@raiyi.com
 */
public class MD5Utils {
    private static final String MD5_CHARSET = "UTF-8";

    /**
     * MD5加密
     *
     * @param sStr
     * @return
     */
    public static String EncoderByMd5(String sStr) {
        String sReturnCode = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(sStr.getBytes(MD5_CHARSET));
            byte b[] = md.digest();
            int i;
            StringBuffer sb = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0) {
                    i += 256;
                }
                if (i < 16) {
                    sb.append("0");
                }
                sb.append(Integer.toHexString(i));
            }

            sReturnCode = sb.toString().toUpperCase();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        return sReturnCode;
    }
}
