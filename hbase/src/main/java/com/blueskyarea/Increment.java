package com.blueskyarea;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Increment {

	public static void main(String[] args) {
		// Target keys
		List<String> targetKeys = new ArrayList<String>();
		targetKeys.add("key1");
		targetKeys.add("key2");
		targetKeys.add("key3");
		targetKeys.add("key4");
		targetKeys.add("key5");
		targetKeys.add("saveKeysaveData1");
		targetKeys.add("saveKeysaveData2");
		
		// Filter
		FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		for (String key : targetKeys){
			filter.addFilter(new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(key))));
		}
		
		//BinaryComparator comparator =
		//		new BinaryComparator(Bytes.toBytes("saveKeysaveData1"));
		//	Filter filter = new RowFilter(CompareOp.EQUAL, comparator);
		
		// configure HBase
		Configuration conf = HBaseConfiguration.create();
		/*Job job;
		try {
			job = Job.getInstance(conf);
			TableMapReduceUtil.initTableMapperJob(
					"ns:test",
					new Scan().setFilter(filter),
					TableMapper.class,
					null,
					null,
					job);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		conf.set(TableInputFormat.INPUT_TABLE, "ns:test");
		String stringScan;
		try {
			stringScan = convertScanToString(new Scan().setFilter(filter));
			System.out.println(stringScan);
			conf.set(TableInputFormat.SCAN, stringScan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		// Initializing the spark context
		SparkConf sparkConf = new SparkConf().setAppName("LoadData").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
		// Load an RDD of (ImmutableBytesWritable, Result) tuples from the table
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc.newAPIHadoopRDD(conf,
				TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		
		// Count data size
		System.out.println("data size:" + hbaseRDD.count());
		
		// Get data from hbase
		
		// Adding

		// Save data to hbase
	}
	
	/*private static String convertScanToString(Scan scan) {
		try {
			return Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}*/
	
//	static String convertScanToString(Scan scan) throws IOException {
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        DataOutput dos = new DataOutputStream(out);
//        scan.write(dos);
//        return Base64.encodeBytes(out.toByteArray());
//    }
	
	static String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}


}