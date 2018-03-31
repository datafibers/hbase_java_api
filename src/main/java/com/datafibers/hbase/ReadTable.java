/**
 * 
 */
package com.datafibers.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.datafibers.hbase.util.HBaseConfigUtil;

/**
 * @author datafibers
 *
 */
public class ReadTable {

	public static void main(String[] args) {
		ReadTable readTable = new ReadTable();
		readTable.readTableData(args[0]);
	}

	public void readTableData(String rowKey) {
		Configuration config = HBaseConfigUtil.getHBaseConfiguration();

		Connection connection = null;
		Table table = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf("peoples"));

			// Instantiating Get class
			Get get = new Get(Bytes.toBytes(rowKey));

			// Reading the data
			Result result = table.get(get);

			// Reading values from Result class object
			byte[] firstNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("first"));
			byte[] lastNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("last"));
			byte[] emailValue = result.getValue(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
			byte[] genderValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"));
			byte[] ageValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));

			// Printing the values
			String firstName = Bytes.toString(firstNameValue);
			String lastName = Bytes.toString(lastNameValue);
			String email = Bytes.toString(emailValue);
			String gender = Bytes.toString(genderValue);
			String age = Bytes.toString(ageValue);

			System.out.println("First Name : " + firstName + " --- Last Name : " + lastName + " --- Email : " + email + " --- Gender : " + gender + " --- Age : " + age);
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
