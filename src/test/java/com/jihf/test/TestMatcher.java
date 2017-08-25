package com.jihf.test;

import com.jihf.mr.utils.Matcher;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class TestMatcher {
    private final static Matcher matcher = new Matcher(true);

    @Test
    public void testMatcher() {

        // hosts.txt说明
        // 1.url为大写字母（www.qq.com --> WWW.QQ.COM）(有www的不能省略)
        // 2.没有协议头（http://www.qq.com --> WWW.QQ.COM）
        // 3.没有参数（http://www.qq.com？a=b --> WWW.QQ.COM）
        // 4.域名完全匹配（如果hosts.txt中为WWW.QQ.COM，那么匹配dpi记录时，依次匹配destination/host/domain字段）
        // 5.通配符使用（尽量少使用通配符，通配符位置尽量靠后，不要出现连续的通配符如**）
        // hosts.txt dpi
        // WWW.QQ.COM --> www.qq.com
        // *.QQ.COM --> www.qq.com, film.qq.com, history.news.qq.com
        // V.QQ.COM/*/VARIETY --> v.qq.com/x/list/variety
        // V.QQ.COM/VPLUS/* --> v.qq.com/vplus/miss, v.qq.com/vplus/miss/videos

        matcher.addPattern("*12300*", 100);

        matcher.addPattern("WWW.QQ.COM", 200);
        matcher.addPattern("*.QQ.COM", 300);
        matcher.addPattern("V.QQ.COM/*/VARIETY", 400);
        matcher.addPattern("V.QQ.COM/VPLUS/*", 500);
        matcher.addPattern("v.QQ.COM/VPLUS", 600);
        Matcher.MatchResult[] q = matcher.match( "12300213123");
        for (Matcher.MatchResult result : q) {
            System.out.println(String.format("%s|%s|%s",  "2-233312300213123", result.data.toString(), result.pattern));
        }
//        String[] dpiUrls = {"WWW.qq.com", "film.qq.com"
//                , "history.news.qq.com"
//                , "v.qq.com/x/list/variety"
//                , "v.qq.com/x/variety"
//                , "v.qq.com/vplus"
//                , "v.qq.com/vplus/"
//                , "v.qq.com/vplus/miss"
//                , "v.qq.com/vplus/miss/videos?a=x"
//                , "http://v.qq.com/vplus/miss/folders"
//                };
//
//        for (String dpiUrl : dpiUrls) {
//
//            if (StringUtils.isNotBlank(dpiUrl)) {
//                String url = dpiUrl.toUpperCase();
//                int index = url.indexOf("HTTP://");
//                if (index != -1) {
//                    url = url.substring(index + 7);
//                }
//                index = url.indexOf("?");
//                if (index != -1) {
//                    url = url.substring(0, index);
//                }
//                Matcher.MatchResult[] results = matcher.match(url);
//                for (Matcher.MatchResult result : results) {
//                    System.out.println(String.format("%s|%s|%s", dpiUrl, result.data.toString(), result.pattern));
//                }
//            }
//        }
    }
}
