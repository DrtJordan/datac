package com.jihf.test;

import com.jihf.mr.utils.Matcher;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestMatcher {
    private final static Matcher matcher = new Matcher(true);
    private static List<String> matchHostList = new ArrayList<String>();

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


        if (matchHostList.size() == 0) {
            matchHostList.add("*.xinstatic.com*");
            matchHostList.add("*.xin.com*");

        }
        for (int i = 0; i < matchHostList.size(); i++) {
            matcher.addPattern(matchHostList.get(i), i);
        }

//
//        String host = "ocjstestdomain.tcdn.qq.com";
//        System.out.println(host);
//        Matcher.MatchResult[] matchResults = matcher.match(host);
//        System.out.println(matchResults.length);
//        for (Matcher.MatchResult result : matchResults) {
//            System.out.println(String.format("%s|%s", result.pattern, result.data.toString()));
//        }

        System.out.println("matchHostList：" + matchHostList.size());
        String[] dpiUrls = {
                "http://app.xin.com/api/package",
                "https://api.xin.com/car_search/search",
                "http://c1.xinstatic.com/f1/20170830/1701/59a67efba4093701741_19.jpg",
                "https://api.xin.com/home/get_tabbar_icon"
        };

        for (String dpiUrl : dpiUrls) {

            if (StringUtils.isNotBlank(dpiUrl)) {
                String url = dpiUrl;
                int index = -1;
                if (url.startsWith("http://")) {
                    index = 7;
                } else if (url.startsWith("https://")) {
                    index = 8;
                }
                url = url.substring(index);
                index = url.indexOf("?");
                if (index != -1) {
                    url = url.substring(0, index);

                }
                System.out.println(url);
                Matcher.MatchResult[] results = matcher.match(url);
                for (Matcher.MatchResult result : results) {
                    System.out.println(String.format("%s|%s|%s", dpiUrl, result.data.toString(), result.pattern));
                }
            }
        }
    }
}
