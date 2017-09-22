package com.jihf.mr.java;

import java.sql.*;

/**
 * Func：
 * Desc:
 * Author：JHF
 * Data：2017-09-22 10:41
 * Mail：jihaifeng@raiyi.com
 */
public class JdbcPostgre {

    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        try {
            //换成自己PostgreSQL数据库实例所在的ip地址，并设置自己的端口
            String url = "jdbc:postgresql://10.99.4.167:5432/usertag";
            String userName = "postgres";
            String passWord = "raiyi520";
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(url, userName, passWord);
            System.out.println("connect pgSql = " + connection);
            String sql  = "SELECT * FROM bd_tag_host";
            statement = connection.createStatement();
            /**
             * 关于ResultSet的理解：Java程序中数据库查询结果的展现形式，或者说得到了一个结果集的表
             * 在文档的开始部分有详细的讲解该接口中应该注意的问题，请阅读JDK
             * */
            ResultSet resultSet = statement.executeQuery(sql);
            while(resultSet.next()){
                //取出列值
                String tagCode = resultSet.getString(1);
                String hostName = resultSet.getString(2);
                String link = resultSet.getString(3);
                String createTime = resultSet.getString(4);
                System.out.println(tagCode+":"+hostName+",");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (null != statement) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                try {
                    if (null != connection) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

        }
    }
}
