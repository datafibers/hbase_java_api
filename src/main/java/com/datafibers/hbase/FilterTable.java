/**
 * 
 */
package com.datafibers.hbase;

import com.datafibers.hbase.util.HBaseConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author datafibers
 *
 */
public class FilterTable {

	public static void main(String[] args) {
		Configuration config = HBaseConfigUtil.getHBaseConfiguration();

		Connection connection = null;
		Table table = null;
		ResultScanner resultScanner = null;

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf("peoples"));

			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"), CompareOp.EQUAL, Bytes.toBytes("F"));
			SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("25"));

			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			filterList.addFilter(filter1);
			filterList.addFilter(filter2);

			Scan scan = new Scan();
			scan.setFilter(filterList);
			scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"));
			scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"));
			scan.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
			scan.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"));
			scan.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));

			resultScanner = table.getScanner(scan);

			for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
				byte[] firstNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("first"));
				byte[] lastNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("last"));
				byte[] emailValue = result.getValue(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
				byte[] genderValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"));
				byte[] ageValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));

				String firstName = Bytes.toString(firstNameValue);
				String lastName = Bytes.toString(lastNameValue);
				String email = Bytes.toString(emailValue);
				String gender = Bytes.toString(genderValue);
				String age = Bytes.toString(ageValue);

				System.out.println("First Name : " + firstName + " --- Last Name : " + lastName + " --- Email : " + email + " --- Gender : " + gender + " --- Age : " + age);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (resultScanner != null) {
					resultScanner.close();
				}

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
