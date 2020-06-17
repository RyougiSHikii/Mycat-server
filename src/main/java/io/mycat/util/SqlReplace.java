package io.mycat.util;

import org.apache.commons.lang.ArrayUtils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author violet
 */
public abstract class SqlReplace {
    private Pattern pattern;
    private String charset;

    public SqlReplace(Pattern pattern, String charset) {
        this.pattern = pattern;
        this.charset = charset;
    }

    /**
     * 替换sql,解决sql在不同数据库不兼容问题
     *
     * @param data 原sql
     * @return 是否处理过
     */
    public byte[] replaceSql(byte[] data) {
        try {
            String sql = getSql(data);

            Matcher matcher = pattern.matcher(sql);

            StringBuffer sb = new StringBuffer();
            int end = 0;
            while (matcher.find()) {
                matcher.appendReplacement(sb, replacement(matcher));
                end = matcher.end();
            }
            if (sb.length() == 0) {
                return data;
            }
            sb.append(sql.substring(end));

            return ArrayUtils.addAll(Arrays.copyOf(data, 5), sb.toString().getBytes(charset));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return data;
        }
    }

    /**
     * 替换sql,解决sql在不同数据库不兼容问题
     *
     * @param matcher 匹配到的词
     */
    public abstract String replacement(Matcher matcher);

    private String getSql(byte[] data) throws UnsupportedEncodingException {
        return new String(data, 5, data.length - 5, charset);
    }
}
