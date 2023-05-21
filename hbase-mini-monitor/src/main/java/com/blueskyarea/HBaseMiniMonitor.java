package com.blueskyarea;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseMiniMonitor {
    public static void main( String[] args ) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    	Configuration hbaseConf = HBaseConfiguration.create();
    	hbaseConf.set("hbase.zookeeper.quorum", "localhost");
    	Connection hConnection = ConnectionFactory.createConnection(hbaseConf);
        Admin admin = hConnection.getAdmin();
        
        System.out.println(admin.getMasterInfoPort());
        System.out.println(admin.getClusterStatus().getHBaseVersion());
        System.out.println(admin.getClusterStatus().getRegionsCount());
        Collection<ServerName> servers = admin.getClusterStatus().getServers();
        for(ServerName server : servers) {
        	System.out.println(server.getHostname());
        }
    }
}
