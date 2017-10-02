package com.sailabs.hbase;

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class HBaseClient {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Configuration con = HBaseConfiguration.create();
		con.set("hbase.zookeeper.property.clientPort", "2181");
		con.set("hbase.zookeeper.quorum", "nn01.itversity.com");
		con.set("zookeeper.znode.parent", "/hbase-unsecure");

		try {

			Connection conn = ConnectionFactory.createConnection();

			TableName tablename = TableName.valueOf("mytable");
			Table t = conn.getTable(tablename);
			// t.put(new Put(row).add(fam, qual, value));t.close();

			Admin admin = conn.getAdmin();
			HTableDescriptor[] tableDescriptor = admin.listTables();

			for (int i = 0; i < tableDescriptor.length; i++) {
				System.out.println(tableDescriptor[i].getNameAsString());
			}

			// Instantiating table descriptor class
			HTableDescriptor tableDescriptor1 = admin.getTableDescriptor(tablename);
			admin.createTable(tableDescriptor1);
			
			// Adding column families to table descriptor
			tableDescriptor1.addFamily(new HColumnDescriptor("personal"));
			tableDescriptor1.addFamily(new HColumnDescriptor("professional"));
			// Execute the table through admin
			admin.createTable(tableDescriptor1);
			System.out.println(" Table created ");

		} catch (Exception e) {
			Logger.getLogger("HBase" + e.toString());

		}

	}

}
