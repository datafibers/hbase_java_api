/**
 * 
 */
package com.datafibers.hbase;

import java.util.ArrayList;
import java.util.List;

import com.datafibers.hbase.util.HBaseConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author datafibers
 *
 */
public class DeleteRecordsFromTable {

	public static void main(String[] args) {
		Configuration config = HBaseConfigUtil.getHBaseConfiguration();

		Connection connection = null;
		Table table = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf("peoples"));

			List<Delete> deleteList = new ArrayList<Delete>();

			for (int rowKey = 1; rowKey <= 10; rowKey++) {
				deleteList.add(new Delete(Bytes.toBytes(rowKey + ""))); // here use rowKey + "" make it as string
			}

			table.delete(deleteList);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null) {
					table.close();
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
