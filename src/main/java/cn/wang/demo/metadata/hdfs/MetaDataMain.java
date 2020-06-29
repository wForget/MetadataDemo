package cn.wang.demo.metadata.hdfs;

import cn.wang.demo.metadata.conf.ConfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class MetaDataMain {

    public static void main(String[] args) throws URISyntaxException, IOException {
        System.setProperty("HADOOP_USER_NAME", "hive");
        // hadoop fs ls /user/${user}
        String hdfsUri = ConfUtils.getString("hdfs.url");
        URI uri = new URI(hdfsUri);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(uri, configuration);

        System.out.println("--------listFiles-------");
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/user/hive"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println(fileStatus);
        }

        System.out.println("--------listStatus-------");
        Arrays.stream(fileSystem.listStatus(new Path("/user/hive"))).forEach(System.out::println);

    }

}
