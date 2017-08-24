package com.jihf.mr.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;

/**
 * Created by ben on  17-5-26..
 * 字符树匹配
 */
public class Matcher implements Serializable {
    public Matcher(boolean isDebug) {
        this.isDebug = isDebug;
    }

    private boolean isDebug;
    protected static Logger logger = Logger.getLogger(Matcher.class);


    class MatchContext implements Serializable {
        Set<Integer> matchedNodes = new TreeSet<Integer>();
        List<MatchResult> results = new ArrayList<MatchResult>();
        String path;
    }

    public Map<Character, Node> nodes = new TreeMap<Character, Node>();
    private MatchContext context = new MatchContext();

    public static class MatchResult implements Serializable {
        public MatchResult(String pattern, Object data) {
            this.pattern = pattern;
            this.data = data;
        }

        public String pattern;
        public Object data;
        static MatchResult[] EMPTY = new MatchResult[0];


    }

    class Node implements Serializable {
        public Node(String pattern, char name, Node parent) {
            this.pattern = pattern;
            this.name = name;
            this.parent = parent;
        }

        private String pattern;
        private Node parent;
        private char name;
        private Map<Character, Node> nodes = null;
        private List<Object> data = null;

        void addPattern(String pattern, int idx, Object data) {

            if (idx + 1 < pattern.length()) {
                if (nodes == null)
                    nodes = new TreeMap<Character, Node>();
                Node n;
                char c = pattern.charAt(idx + 1);
                if (nodes.containsKey(c))
                    n = nodes.get(c);
                else {
                    n = new Node(pattern, c, this);
                    nodes.put(c, n);
                }
                n.addPattern(pattern, idx + 1, data);
            } else if (pattern.length() == idx + 1) {
                if (isDebug)
                    logger.debug(String.format("Leaf node pattern=%s, name=%s", new String(pattern), name));
                if (this.data == null)
                    this.data = new LinkedList<Object>();
                this.data.add(data);
                //如果QQ.COM.CN在QQ.COM在之前addPattern，QQ.COM进行匹配时，返回的pattern是QQ.COM.CN
                this.pattern = pattern;
            } else {
                throw new IllegalStateException(String.format("Unexpected index(%s) of pattern:%s", idx, pattern));
            }
        }

        void buildString(StringBuffer output, String prefix) {
            if (data != null) {
                output.append(prefix);
                output.append(name);
                output.append(" --->>> ");
                output.append(StringUtils.join(data, ','));
                output.append('\n');
            }
            prefix += name;
            if (nodes != null)
                for (Node n : nodes.values()) {
                    n.buildString(output, prefix);
                }
        }

        void find(char[] chars, int idx) {
//            logger.info(String.format("%s/%s", idx, chars.length));
            if (idx < chars.length) {
                if (nodes != null) {
                    char c = chars[idx];

                    if (nodes.containsKey(c)) {
                        Node n = nodes.get(c);
                        if (isDebug)
                            logger.debug(String.format("%s [FIND] %s <%s>", context.path, context.path.substring(0, idx + 1), n.getRoute()));
                        n.find(chars, idx + 1);
                    }

                    if (nodes.containsKey('*')) {
                        Node anyNode = nodes.get('*');
                        for (int index = idx; index <= chars.length; index++) {
                            if (isDebug)
                                logger.debug(String.format("%s [FIND] %s <%s>", context.path, context.path.substring(0, index), anyNode.getRoute()));
                            anyNode.find(chars, index);
                        }
                    }

//                    if (name == '*') {
//                        logger.info(String.format("%s.%s", this.results, getRoute()));
//                        find(chars, idx + 1);
//                    }
                }
            } else {
//                logger.info(String.format("%s %s", name, results));
                if (nodes != null)
                    if (nodes.containsKey('*')) {
                        Node anyNode = nodes.get('*');
                        if (isDebug)
                            logger.debug(String.format("%s [FIND] %s <%s>", context.path, context.path.substring(0, idx), anyNode.getRoute()));
                        anyNode.find(chars, chars.length);
                    }
                if (data != null) {
                    if (isDebug)
                        logger.info(String.format("%s [HIT] %s", context.path, getRoute()));
                    for (int i = 0; i < data.size(); i++) {
                        context.results.add(new MatchResult(pattern, data.get(i)));
                    }
                }
            }
        }

        private String getRoute() {
            return parent == null ? String.valueOf(name) : (parent.getRoute() + name);
        }
    }


    public void addPattern(String pattern, Object data) {
        if (pattern.length() == 0)
            throw new IllegalArgumentException("Pattern must not be empty");

        char c = pattern.charAt(0);
        Node n;
        if (nodes.containsKey(c)) {
            n = nodes.get(c);
        } else {
            n = new Node(pattern, c, null);
            nodes.put(c, n);
        }
        n.addPattern(pattern, 0, data);
    }


    @Override
    public String toString() {
        StringBuffer bf = new StringBuffer();
        for (Node n : nodes.values()) {
            n.buildString(bf, "");
        }
        return bf.toString();
    }

    public MatchResult[] match(String path) {
        if (null == path || "".equals(path))
            return MatchResult.EMPTY;

        context.matchedNodes.clear();
        context.results.clear();
        context.path = path;
        char[] chars = path.toCharArray();

        char c = chars[0];
        if (nodes.containsKey(c)) {
            if (isDebug)
                logger.debug(String.format("%s [FIND] %s <%s>", context.path, context.path.substring(0, 1), c));
            nodes.get(c).find(chars, 1);
        }
        if (nodes.containsKey('*')) {
            Node anyNode = nodes.get('*');
            for (int index = 0; index <= chars.length; index++) {
                if (isDebug)
                    logger.debug(String.format("%s [FIND] %s <%s>", context.path, context.path.substring(0, index), anyNode.getRoute()));
                anyNode.find(chars, index);
            }
        }
        return context.results.toArray(MatchResult.EMPTY);
    }
}