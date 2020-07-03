package cn.wang.demo.metadata.mysql;

import cn.wang.demo.metadata.conf.ConfUtils;

import java.sql.*;

public class MetaDataMain {

//    public static void init() {
//        try {
//            String driverName = "com.mysql.cj.jdbc.Driver";
//            Class.forName(driverName);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//    static {
//        init();
//    }

    public static void main(String[] args) throws SQLException {
        String url = ConfUtils.getString("jdbc.url");
        String user = ConfUtils.getString("jdbc.username");
        String password = ConfUtils.getString("jdbc.password");
        Connection connection = DriverManager.getConnection(url, user, password);

        System.out.println("--------getCatalogs----------");
        printResultSet(connection.getMetaData().getCatalogs());

        System.out.println("--------getSchemas----------");
        printResultSet(connection.getMetaData().getSchemas());

        System.out.println("--------getTables exchangis----------");
        printResultSet(connection.getMetaData().getTables("exchangis", null, null, null));

        System.out.println("--------getColumns exchangis.exchangis_data_source_model----------");
        printResultSet(connection.getMetaData().getColumns("exchangis", null
                , "exchangis_data_source_model", null));

        System.out.println("--------getFunctions----------");
        printResultSet(connection.getMetaData().getFunctions(null, null, null));

        System.out.println("--------table metadata----------");
        String sql = "select * from exchangis_data_source_model limit 1";
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
