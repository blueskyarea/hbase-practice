package com.blueskyarea;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.Test;

public class HBaseTest {
	private static HBaseTestingUtility testUtil = new HBaseTestingUtility();
	
	@Test
	public void testStartMiniCluster() {
		try {
			int numMasters = 1;
			int numSlaves = 3;
			StartMiniClusterOption option = StartMiniClusterOption.builder()
			        .numMasters(numMasters).numRegionServers(numSlaves).numDataNodes(numSlaves).build();
			
			testUtil.startMiniCluster(option);
			List<MasterThread> mThreads = testUtil.getHBaseCluster().getMasterThreads();
			List<RegionServerThread> rThreads = testUtil.getHBaseCluster().getRegionServerThreads();
			assertThat(mThreads.size(), is(1));
			assertThat(rThreads.size(), is(3));
			testUtil.shutdownMiniCluster();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
