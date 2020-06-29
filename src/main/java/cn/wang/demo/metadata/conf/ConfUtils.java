package cn.wang.demo.metadata.conf;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ConfUtils {

    private static final String CONFIG_RESOURCE_NAME = "metadata.conf";
    private static final Config config = ConfigFactory.load(CONFIG_RESOURCE_NAME);

    public static String getString(String key) {
        return config.getString(key);
    }

    public static int getInt(String key) {
        return config.getInt(key);
    }

}
