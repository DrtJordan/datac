package com.jihf.mr.mapReduce.sort;

//arraylist中的自定义对象
public class VisitInfo {
    private Long key;
    private String val;

    public VisitInfo() {
        // TODO Auto-generated constructor stub
    }

    public VisitInfo(Long key, String val) {
        this.key = key;
        this.val = val;

    }

    public Long getKey() {
        return key;
    }

    public void setKey(Long key) {
        this.key = key;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }
}