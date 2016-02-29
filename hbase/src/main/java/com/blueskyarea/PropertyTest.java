package com.blueskyarea;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class PropertyTest {
    
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.client.retries.number", "1");
		conf.set("zookeeper.recovery.retry", "1");
		//hbase.rpc.timeout
		//zookeeper.session.timeout
		//zookeeper.recovery.retry.intervalmill
		//hbase.client.pause
		//hbase.master.lease.period
		//hbase.regionserver.lease.period
		//zookeeper.retries
		//zookeeper.pause
		HTable table = new HTable(conf, "ns:test");
		String rowKey = args[0];
		Result result = table.get(new Get(rowKey.getBytes()));
		
		System.out.println(result);
		
		//[reference]
		//http://hbase.apache.org/0.94/book/trouble.client.html
		//http://wiki.apache.org/hadoop/ZooKeeper/FAQ	
		//http://hbase-perf-optimization.blogspot.jp/2013/03/hbase-configuration-optimization.html
		//http://oss.infoscience.co.jp/hadoop/zookeeper/docs/current/zookeeperProgrammers.html#ch_zkSessions
		//http://www.ne.jp/asahi/hishidama/home/tech/apache/hbase/hbase-site_xml.html
		//http://oss.infoscience.co.jp/hbase/book/client_dependencies.html
		//http://hadoop-hbase.blogspot.jp/2012/09/hbase-client-timeouts.html
		
	}
}
