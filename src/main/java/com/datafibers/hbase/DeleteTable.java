package com.datafibers.hbase;

import com.datafibers.hbase.util.HBaseConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @author Prasad Khode
 *
 */
public class DeleteTable {

	public static void main(String[] args) {
		Configuration config = HBaseConfigUtil.getHBaseConfiguration();

		Connection connection = null;
		Admin admin = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			admin = connection.getAdmin();

			TableName tableName = TableName.valueOf("peoples");

			if (admin.isTableAvailable(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}

				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}
