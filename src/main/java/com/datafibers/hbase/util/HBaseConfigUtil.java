/**
 * 
 */
package com.datafibers.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @author Prasad Khode
 *
 */
public class HBaseConfigUtil {
	public static Configuration getHBaseConfiguration() {
		Configuration configuration = HBaseConfiguration.create();
		//configuration.set("hbase.zookeeper.quorum", "localhost");
		//configuration.set("hbase.zookeeper.property.clientPort", "2181");
		//configuration.set("hbase.master","localhost:16000");

		// We can also read the config from files below
		configuration.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
		configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		return configuration;
	}
}
