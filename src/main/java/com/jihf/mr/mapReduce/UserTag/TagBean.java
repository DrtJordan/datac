package com.jihf.mr.mapReduce.UserTag;

import java.util.List;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-25 09:00
 * Mail：jihaifeng@raiyi.com
 */
public class TagBean {
    public List<TagDataBean> tagData;

    public static class TagDataBean {
        /**
         * tagcode : education
         * hostname : gymboglobal.com.cn
         */

        public String tagcode;
        public String hostname;
    }
}
