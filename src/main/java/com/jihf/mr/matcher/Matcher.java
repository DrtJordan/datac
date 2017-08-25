//package com.jihf.mr.matcher;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.log4j.Logger;
//
//import java.io.Serializable;
//import java.util.*;
//
///**
// * Created by ben on  17-5-26..
// * 字符树匹配
// */
//public class Matcher implements Serializable {
//
//
//    private boolean isDebug;
//
//    public Map<Character, Node> nodeMap = new TreeMap<Character, Node>();
//    private MatchContext context = new MatchContext();
//
//
//    public Matcher(boolean isDebug) {
//        this.isDebug = isDebug;
//    }
//
//    public void addPattern(String pattern, Object data) {
//        if (pattern.length() == 0)
//            throw new IllegalArgumentException("Pattern must not be empty");
//
//        char c = pattern.charAt(0);
//        Node n;
//        if (nodeMap.containsKey(c)) {
//            n = nodeMap.get(c);
//        } else {
//            n = new Node(pattern, c, null);
//            nodeMap.put(c, n);
//        }
//        n.addPattern(pattern, 0, data);
//        System.out.println(n.data);
//    }
//
//    public MatchResult[] match(String path) {
//        if (null == path || "".equals(path))
//            return MatchResult.EMPTY;
//
//        context.matchedNodes.clear();
//        context.results.clear();
//        context.path = path;
//        char[] chars = path.toCharArray();
//
//        char c = chars[0];
//        if (nodeMap.containsKey(c)) {
//            nodeMap.get(c).find(chars, 1);
//        }
//        if (nodeMap.containsKey('*')) {
//            Node anyNode = nodeMap.get('*');
//            for (int index = 0; index <= chars.length; index++) {
//                anyNode.find(chars, index);
//            }
//        }
//        return context.results.toArray(MatchResult.EMPTY);
//    }
//
//    @Override
//    public String toString() {
//        StringBuffer bf = new StringBuffer();
//        for (Node n : nodeMap.values()) {
//            n.buildString(bf, "");
//        }
//        return bf.toString();
//    }
//
//    class Node implements Serializable {
//
//
//        Node(String pattern, char name, Node parent) {
//            this.pattern = pattern;
//            this.name = name;
//            this.parent = parent;
//        }
//
//        private String pattern;
//        private Node parent;
//        private char name;
//        private Map<Character, Node> nodes = null;
//        private List<Object> data = null;
//
//        void addPattern(String pattern, int idx, Object data) {
//
//            if (idx + 1 < pattern.length()) {
//                if (nodes == null)
//                    nodes = new TreeMap<Character, Node>();
//                Node n;
//                char c = pattern.charAt(idx + 1);
//                if (nodes.containsKey(c))
//                    n = nodes.get(c);
//                else {
//                    n = new Node(pattern, c, this);
//                    nodes.put(c, n);
//                }
//                n.addPattern(pattern, idx + 1, data);
//            } else if (pattern.length() == idx + 1) {
//                if (this.data == null)
//                    this.data = new LinkedList<Object>();
//                this.data.add(data);
//                //如果QQ.COM.CN在QQ.COM在之前addPattern，QQ.COM进行匹配时，返回的pattern是QQ.COM.CN
//                this.pattern = pattern;
//            } else {
//                throw new IllegalStateException(String.format("Unexpected index(%s) of pattern:%s", idx, pattern));
//            }
//        }
//
//        void buildString(StringBuffer output, String prefix) {
//            if (data != null) {
//                output.append(prefix);
//                output.append(name);
//                output.append(" --->>> ");
//                output.append(StringUtils.join(data, ','));
//                output.append('\n');
//            }
//            prefix += name;
//            if (nodes != null)
//                for (Node n : nodes.values()) {
//                    n.buildString(output, prefix);
//                }
//        }
//
//        void find(char[] chars, int idx) {
////            logger.info(String.format("%s/%s", idx, chars.length));
//            if (idx < chars.length) {
//                if (nodes != null) {
//                    char c = chars[idx];
//
//                    if (nodes.containsKey(c)) {
//                        Node n = nodes.get(c);
//                        n.find(chars, idx + 1);
//                    }
//
//                    if (nodes.containsKey('*')) {
//                        Node anyNode = nodes.get('*');
//                        for (int index = idx; index <= chars.length; index++) {
//                            anyNode.find(chars, index);
//                        }
//                    }
//
////                    if (name == '*') {
////                        logger.info(String.format("%s.%s", this.results, getRoute()));
////                        find(chars, idx + 1);
////                    }
//                }
//            } else {
////                logger.info(String.format("%s %s", name, results));
//                if (nodes != null)
//                    if (nodes.containsKey('*')) {
//                        Node anyNode = nodes.get('*');
//                        anyNode.find(chars, chars.length);
//                    }
//                if (data != null) {
//                    for (int i = 0; i < data.size(); i++) {
//                        context.results.add(new MatchResult(pattern, data.get(i)));
//                    }
//                }
//            }
//        }
//
//        private String getRoute() {
//            return parent == null ? String.valueOf(name) : (parent.getRoute() + name);
//        }
//
//
//    }
//
//}