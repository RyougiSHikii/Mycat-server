package demo.test;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mycat
 */
public class TestClass1 {

    public static void main(String args[]) throws SQLException, ClassNotFoundException {
        String jdbcdriver = "com.mysql.jdbc.Driver";
        String jdbcurl = "jdbc:mysql://127.0.0.1:8066/TESTDB?useUnicode=true&characterEncoding=utf-8";
        String username = "test";
        String password = "test";
        System.out.println("开始连接mysql:" + jdbcurl);
        Class.forName(jdbcdriver);
        Connection c = DriverManager.getConnection(jdbcurl, username, password);
        Statement st = c.createStatement();
        print("test jdbc ", st.executeQuery("select count(*) from travelrecord "));
        System.out.println("OK......");
    }

    static void print(String name, ResultSet res)
            throws SQLException {
        System.out.println(name);
        ResultSetMetaData meta = res.getMetaData();
        //System.out.println( "\t"+res.getRow()+"条记录");
        String str = "";
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            str += meta.getColumnName(i) + "   ";
            //System.out.println( meta.getColumnName(i)+"   ");
        }
        System.out.println("\t" + str);
        str = "";
        while (res.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                str += res.getString(i) + "   ";
            }
            System.out.println("\t" + str);
            str = "";
        }
    }

    @Test
    public void timestamp() {
        String sql = "Insert into ERM_USER_TOKEN (LOGID,TOKEN,USERNAME,LASTACCESSEDTIME,LOGINSUCCEEDTIME,REMOTEADDR,LASTALIVETIME,USERREALNAME,LOCATION,AUTHORITIES,EXTAG) values (ERM_SEQUENCE.nextval,'ASyutJwZPSVUmmCbmKMm'," +
                "'yga','2020-03-16 15:56:06.093','2020-03-16 15:56:06.093','127.0.0.1',null,'员工A','ERM','[\"ROLE_ROLEPM\",\"ROLE_ALLUSER\",\"USER_yga\",\"ROLE_ipo\",\"ROLE_ROLETPL\",\"ROLE_ROLEDPM\"]',null)";

        String sql1 = "update ERM_USER_TOKEN set TOKEN='POsjqzJWICINJvPUqaRE', USERNAME='yga'," +
                " LASTACCESSEDTIME='2020-03-16 17:34:52.3333', LOGINSUCCEEDTIME='2020-03-16 16:15:51.1', REMOTEADDR='127.0.0.1', LASTALIVETIME=null, USERREALNAME='员工A', LOCATION='rdm', AUTHORITIES='[\"ROLE_ROLEPM\",\"ROLE_ALLUSER\",\"USER_yga\",\"ROLE_ipo\",\"ROLE_ROLETPL\",\"ROLE_ROLEDPM\"]', EXTAG=null where LOGID=36589";
        try {
            System.out.println(new String(handleTimestamp(sql1.getBytes(charset)), charset));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }


    private String getSql(byte[] data) throws UnsupportedEncodingException {
        return new String(data, 5, data.length - 5, charset);
    }


    @Test
    public void replaceSequence() {
        String sql = "123456 INSERT INTO ERM_USER_TOKEN ( LOGID, TOKEN, USERNAME, LASTACCESSEDTIME, LOGINSUCCEEDTIME, REMOTEADDR, LASTALIVETIME, USERREALNAME, LOCATION, AUTHORITIES, EXTAG )\n" +
                "VALUES(ERM_SEQUENCE.nextval, 'ffOnyZbXlcTwihGCexEJ', abc.nextval,'yga', xxx.nextval,'2020-03-20 13:43:59.532', '2020-03-20 13:43:59.532', '127.0.0.1', NULL, '员工A', 'ERM', '[\"ROLE_ROLEPM\",\"ROLE_ALLUSER\",\"USER_yga\",\"ROLE_ipo\",\"ROLE_ROLETPL\",\"ROLE_ROLEDPM\"]', NULL )";

        replaceSequence(sql.getBytes(StandardCharsets.UTF_8));
    }


    private byte[] replaceSequence(byte[] data) {
        try {
            String sql = getSql(data);

            Matcher matcher = sequencePattern.matcher(sql);

            StringBuffer sb = new StringBuffer();
            int end = 0;
            while (matcher.find()) {
                String[] match = matcher.group().split("\\.");
                if (match.length != 2) {
                    continue;
                }
                matcher.appendReplacement(sb, match[1] + "('" + match[0] + "')");
                end = matcher.end();
            }
            System.out.println(sb + sql.substring(end));

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return data;
        }
        return data;
    }

    private static Pattern sequencePattern = Pattern.compile("[a-zA-Z_]*.nextval", Pattern.CASE_INSENSITIVE);
    private final String charset = "utf-8";
    private static Pattern timestampPattern = Pattern.compile("'\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d(.?\\d*)'");
    private static final Pattern WM_CONNECT_PATTERN = Pattern.compile("(WMSYS\\.)?WM_CONCAT\\s?\\(['\"\\s]*([a-zA-Z_]+)['\"\\s]*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern REGEXP_PATTERN = Pattern.compile("regexp_like\\s?\\(\\s?([a-zA-Z_]+)\\s?,\\s?'(\\S*)'\\s?\\)", Pattern.CASE_INSENSITIVE);

    @Test
    public void regexpTest() {
        String sql = "SELECT   a.STRUCTURE_ID,   a.STRUCTURE_NAME,   a.STRUCTURE_PATH,   a.PARENT_ID,   a.STRUCTURE_TYPE,   a.SOURCE_ID,   a.CREATE_TIME,   a.CREATOR_CODE,   a.UPDATE_TIME,  " +
                " a.UPDATOR_CODE,   a.DELETE_FLAG,   a.CREATOR_REALNAME,   a.UPDATOR_REALNAME,   a.LEAF_FLAG,   b.MODEL_STATE,   " +
                "c.LIFE_STATUS_NAME as MODEL_STATUS_NAME   FROM RDM_MODEL_STRUCTURE a LEFT JOIN RDM_MODEL b ON b.MODEL_ID = a.STRUCTURE_ID   " +
                "LEFT JOIN RDM_LIFECYCLE_STATUS c ON b.MODEL_STATE = c.LIFE_STATUS_ID   WHERE a.DELETE_FLAG = 0        and a.PARENT_ID=?                     " +
                "    AND b.MODEL_ID in ( select NODE_ID FROM RDM_WORK_BAG_IPTTEAM where USER_CODE = 'yga' union  select MODEL_ID" +
                " from RDM_MODEL where " +
                "(MODEL_MANAGER = 'yga' or regexp_like(VERIFY_USERNAME,'yga') or CREATOR_CODE = 'yga' or APPROVAL_USERNAME = 'yga') )      " +
                "     ORDER BY a.CREATE_TIME DESC,a.STRUCTURE_NAME ASC";
        replaceRegexp(sql);
    }

    public void replaceRegexp(String sql) {
        Matcher matcher = REGEXP_PATTERN.matcher(sql);

        StringBuffer sb = new StringBuffer();
        int end = 0;
        while (matcher.find()) {
            //regexp_like(VERIFY_USERNAME,'yga')   VERIFY_USERNAME~'yga'
            matcher.appendReplacement(sb, matcher.group(1) + " ~ " + matcher.group(2));
            end = matcher.end();
        }
        System.out.println(sb + sql.substring(end));
    }


    //regexp_like ( VERIFY_USERNAME, 'yga' )
    @Test
    public void replaceConnect() {
        String sql = "12e1fd SELECT\n" +
                "WMSYS.WM_CONCAT ( \"GROUP_ID\" ) group_id,\n" +
                "LIFE_ACT_CODE\n" +
                "\t\tfrom RDM_ACT join RDM_ACT_GROUP on RDM_ACT.LIFE_ACT_ID=\n" +
                "\t\tRDM_ACT_GROUP.ACT_ID\n" +
                "\t\twhere RDM_ACT.LIFE_ACT_CODE in( 'new_model' )\n" +
                "\t\tGROUP BY LIFE_ACT_CODE";
        System.out.println(new String(replaceConnect(sql.getBytes(StandardCharsets.UTF_8))));
    }

    private byte[] replaceConnect(byte[] data) {
        try {
            String sql = getSql(data);

            Matcher matcher = WM_CONNECT_PATTERN.matcher(sql);

            StringBuffer sb = new StringBuffer();
            int end = 0;
            while (matcher.find()) {
                matcher.appendReplacement(sb, "string_agg(" + matcher.group(2) + ",',')");
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

    public void replace(StringBuffer sb, Matcher matcher) {
        matcher.appendReplacement(sb, "string_agg(" + matcher.group() + ",',')");
    }


    private byte[] handleTimestamp(byte[] data) {
        try {
            String sql = new String(data, 5, data.length - 5, charset);

            boolean match = false;
            Matcher matcher = timestampPattern.matcher(sql);
            StringBuffer sb = new StringBuffer();
            int end = 0;
            while (matcher.find()) {
                matcher.appendReplacement(sb, "to_timestamp(" + matcher.group() + ",'YYYY-MM-DD HH24:MI:SS.ff')");
                match = true;
                end = matcher.end();
            }
            sb.append(sql.substring(end));
            if (match) {
                return ArrayUtils.addAll(Arrays.copyOf(data, 5), sb.toString().getBytes(charset));
            } else {
                return data;
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return data;
        }
    }
}
