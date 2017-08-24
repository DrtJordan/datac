package com.jihf.mr.matcher;

import java.io.Serializable;

public class MatchResult implements Serializable {
    MatchResult(String pattern, Object data) {
        this.pattern = pattern;
        this.data = data;
    }

    private String pattern;
    public Object data;
    static MatchResult[] EMPTY = new MatchResult[0];


}