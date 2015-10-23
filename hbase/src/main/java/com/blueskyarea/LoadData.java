package com.blueskyarea;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

// How to load data from hbase using spark.
public class LoadData {
	public static void main(String[] args) {
		// configure HBase
		Configuration conf = HBaseConfiguration.create();
		conf.set(TableInputFormat.INPUT_TABLE, "ns:test");

		// Initializing the spark context
		SparkConf sparkConf = new SparkConf().setAppName("LoadData").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// Load an RDD of (ImmutableBytesWritable, Result) tuples from the table
		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc.newAPIHadoopRDD(conf,
				TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

		// Count data size
		System.out.println("data size:" + hbaseRDD.count());

		// Transform (row key, row value) tuples into an RDD of Results
		// Return a new RDD by applying a function to all elements of this RDD
		JavaPairRDD<String, String> rowPairRDD = hbaseRDD
				.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
					public Tuple2<String, String> call(Tuple2<ImmutableBytesWritable, Result> entry)
							throws Exception {
						Result result = entry._2;
						String rowKey = Bytes.toString(result.getRow());
						String value = Bytes.toString(result.getValue(Bytes.toBytes("data"),
								Bytes.toBytes("q1")));
						return new Tuple2(rowKey, value);
					}
				});

		// Return an List that contains all of the elements in this RDD
		List<Tuple2<String, String>> rows = rowPairRDD.collect();

		// Check data
		for (Tuple2<String, String> row : rows) {
			System.out.println("key:" + row._1);
			System.out.println("value:" + row._2);
		}
	}
}
