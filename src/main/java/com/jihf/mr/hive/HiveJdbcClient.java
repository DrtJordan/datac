package com.jihf.mr.hive;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {


    private static final String hiveUserName = "zhengzonghui";
    private static final String hiveUserPass = "zhengzonghui";

    // hiverserver 版本使用此驱动 Technorati 标记: hadoop,hive,jdbc
    private static String hiveDriverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    // hiverserver2 版本使用此驱动
    private static String hive2DriverName = "org.apache.hive.jdbc.HiveDriver";

    // http://61.155.179.141:8889/hue/
    private static String hostIp = "61.155.179.141:8889/accounts/login/";

    // hiverserver 版本 jdbc url格式  "jdbc:hive://host:port/default"
    private static String hiveJdbcUrl = String.format("jdbc:hive://%s", hostIp);
    // hiverserver2 版本jdbc url格式 "jdbc:hive2://host:port/default"
    private static String hive2JdbcUrl = String.format("jdbc:hive2://%s", hostIp);


    public static void main(String[] args) throws SQLException {


        try {
//            loadLog4j();
            Class.forName(hive2DriverName);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("hive2JdbcUrl：" + hive2JdbcUrl);

        try {
            Connection con = DriverManager.getConnection(hive2JdbcUrl, hiveUserName, hiveUserPass);
            System.out.println("con：" + con);

            Statement stmt = con.createStatement();
            System.out.println("stmt：" + stmt);

            String sql = "select fs.* from dw_sms.sms_send_record fs limit 100";
            ResultSet res = stmt.executeQuery(sql);

            while (res.next()) {
                System.out.println(res);
            }
        } catch (SQLException e) {
            System.err.println("e：" + e.getMessage());
            e.printStackTrace();
        }

    }

//    private static void loadLog4j() throws IOException {
//        //加载配置文件,建议放在src下面
//        PropertyConfigurator.configure("<a target=_blank href=file://\\log4j.properties">\\log4j.properties</a>");
//        Properties props = new Properties();
//        props.load(HiveJdbcClient.class.getClassLoader().getResourceAsStream("log4j.properties"));
//    }

}