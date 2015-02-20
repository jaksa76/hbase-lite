package me.jaksa.hbase.lite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Responsible for JVM wide setting such as the HBase configuration.
 * At the moment we do not support accessing different HBase instances at the same time.
 * This class is thread safe.
 *
 * @author Jaksa Vuckovic
 */
public class HBaseLite {
    private static Configuration configuration;

    public static synchronized Configuration getConfiguration() {
        if (configuration == null) {
            configuration = HBaseConfiguration.create();
            configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        }
        return configuration;
    }

    public static synchronized void setConfiguration(Configuration configuration) {
        HBaseLite.configuration = configuration;
    }
}
