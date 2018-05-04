package com.datafibers.hbase;

import com.datafibers.hbase.util.HBaseConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author datafibers
 *
 */
public class InsertIntoTable {

	public static void main(String[] args) {
		InsertIntoTable object = new InsertIntoTable();
		object.insertRecords();
	}

	public void insertRecords() {
		Configuration config = HBaseConfigUtil.getHBaseConfiguration();

		Connection connection = null;
		Table table = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf("peoples"));

//			creating sample data that can be used to save into hbase table
			String[][] people = {
					{ "1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26" },
					{ "2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24" },
					{ "3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27" },
					{ "4", "Rae", "Schroeder", "rae@xyz.com", "F", "31" },
					{ "5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25" },
					{ "6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24" } };

			for (int i = 0; i < people.length; i++) {
				Put person = new Put(Bytes.toBytes(people[i][0]));
				person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(people[i][1]));
				person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(people[i][2]));
				person.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes(people[i][3]));
				person.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"), Bytes.toBytes(people[i][4]));
				person.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"), Bytes.toBytes(people[i][5]));
				table.put(person);
			}
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
