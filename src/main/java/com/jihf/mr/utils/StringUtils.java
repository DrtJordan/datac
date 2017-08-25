package com.jihf.mr.utils;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-11 09:12
 * Mail：jihaifeng@raiyi.com
 */
public class StringUtils extends org.apache.commons.lang.StringUtils{
    public static boolean strIsEmpty(String str) {
        return null == str || str.trim().length() <= 0 || str.equals("null");
    }

    public static boolean isNumericZidai(String str) {
        for (int i = 0; i < str.length(); i++) {
            System.out.println(str.charAt(i));
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static String getTelnum(String str) {
        if (str.length() <= 0)
            return "";
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(1|861)(3|5|8)\\d{9}$*");
        java.util.regex.Matcher matcher = pattern.matcher(str);
        StringBuffer bf = new StringBuffer();
        while (matcher.find()) {
            bf.append(matcher.group()).append(",");
        }
        int len = bf.length();
        if (len > 0) {
            bf.deleteCharAt(len - 1);
        }
        return bf.toString();
    }


}
