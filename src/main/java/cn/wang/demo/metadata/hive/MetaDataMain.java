package cn.wang.demo.metadata.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;

public class MetaDataMain {

    public static void main(String[] args) throws TException {
        HiveConf conf = new HiveConf();
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(conf);

        System.out.println("--------getAllDatabases--------");
        List<String> allDatabases = hiveMetaStoreClient.getAllDatabases();
        allDatabases.forEach(System.out::println);

        System.out.println("--------getAllTables--------");
        List<String> allTables = hiveMetaStoreClient.getAllTables(allDatabases.get(0));
        allTables.forEach(System.out::println);

        System.out.println("--------getTable--------");
        Table table = hiveMetaStoreClient.getTable(allDatabases.get(0), allTables.get(0));
        System.out.println(table);

    }

}
