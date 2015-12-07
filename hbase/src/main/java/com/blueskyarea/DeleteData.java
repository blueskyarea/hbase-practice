package com.blueskyarea;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteData {
	private Configuration conf;
	private HTable table;
	private String targetKey;
	
	public DeleteData(String targetKey) {
		conf = HBaseConfiguration.create();
		this.targetKey = targetKey;
	}
	
	private void deleteByKey() throws IOException {
		System.out.println("Start deleteByKey");
		table = new HTable(conf, "ns:test");
		Delete delete = new Delete(Bytes.toBytes(targetKey));
		delete.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("q1"));
		delete.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("q2"));
		table.delete(delete);
	}
	
	public static void main(String[] args) {
		DeleteData deleteData = new DeleteData(args[0]);
		try {
			deleteData.deleteByKey();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Delete Failed");
		}
		System.out.println("Delete Complete!!!");
	}
}
