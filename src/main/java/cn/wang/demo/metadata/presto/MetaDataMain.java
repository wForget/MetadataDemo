package cn.wang.demo.metadata.presto;

import cn.wang.demo.metadata.conf.ConfUtils;

import java.sql.*;

public class MetaDataMain {

    public static void main(String[] args) throws SQLException {
        String host = ConfUtils.getString("presto.host");
        int port = ConfUtils.getInt("presto.port");
        String user = ConfUtils.getString("presto.username");
        String url = "jdbc:presto://" + host + ":" + port;
        Connection connection = DriverManager.getConnection(url, user, null);

        System.out.println("--------getCatalogs----------");
        printResultSet(connection.getMetaData().getCatalogs());

        System.out.println("--------getSchemas----------");
        printResultSet(connection.getMetaData().getSchemas());

        System.out.println("--------getTables hive.test----------");
        printResultSet(connection.getMetaData().getTables("hive", "test", null, null));

        System.out.println("--------getColumns hive.test.wangz_test----------");
        printResultSet(connection.getMetaData().getColumns("hive", "test"
                , "wangz_test", null));

        System.out.println("--------table metadata----------");
        String sql = "select * from hive.test.wangz_test limit 1";
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        printResultSetMetaData(resultSet);
    }

    private static void printResultSet(ResultSet resultSet) throws SQLException {
        if (resultSet == null) return;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int count = resultSetMetaData.getColumnCount();
        for (int i = 1; i < count; i++) {
            System.out.print(resultSetMetaData.getColumnLabel(i) + "\t");
        }
        System.out.println(resultSetMetaData.getColumnLabel(count));
        while (resultSet.next()) {
            for (int i = 1; i < count; i++) {
                System.out.print(resultSet.getString(i) + "\t");
            }
            System.out.println(resultSet.getString(count));
        }
    }

    private static void printResultSetMetaData(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
//        System.out.println(metaData.toString());
        int count = metaData.getColumnCount();
        for (int i = 1; i <= count; i++) {
            System.out.println(metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i));
        }
    }

}
