package com.jihf.mr.java;


import com.jihf.mr.matcher.MatchResult;
import com.jihf.mr.matcher.Matcher;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-08-18 18:10
 * Mail：jihaifeng@raiyi.com
 */
public class test {
    public static void main(String[] args) {
        String path = "http://shmmsns.qpic.cn/mmsns/lVSRwDuDpBLIibaOibZob9IkQ1ia8jPNEx2tibLMl3DosQvcBjgPV0YmzB0bbh0mnBLQwWdyVibic5qdY/150" +
                "?tp=wxpc&length=2208&width=1242&idx=1" +
                "&token=WSEN6qDsKwV8A02w3onOGQYfxnkibdqSOkmHhZGNB4DEreiaqeicTUMddgHicIZHicuJtLMzHfWiciaCN7POibtCMjDGew";
        Matcher m = new Matcher(true);
        m.addPattern("22", "2");
        MatchResult[] matchResult = m.match(path);
        System.out.println(matchResult.length);
        for (MatchResult result : matchResult) {
            System.out.println("result.data：" + result.data);
        }

    }
}
