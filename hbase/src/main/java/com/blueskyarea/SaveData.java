package com.blueskyarea;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SaveData {
	public static void main(String[] args) {
		// configure HBase
		Configuration conf = HBaseConfiguration.create();
		conf.set(TableOutputFormat.OUTPUT_TABLE, "ns:test");

		// Initializing the spark context
		SparkConf sparkConf = new SparkConf().setAppName("SaveData").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		// Define input data		
		List<String> data = new ArrayList();
		data.add("saveData1");
		data.add("saveData2");
		JavaRDD<String> dataRdd = jsc.parallelize(data);

		// new Hadoop API configuration
		try {
			Job job = Job.getInstance(conf);
			job.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			saveToHBase(dataRdd, job.getConfiguration());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void saveToHBase(JavaRDD<String> dataRdd, Configuration conf2) {
		// create Key, Value pair to store in HBase
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = dataRdd
				.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
					public Tuple2<ImmutableBytesWritable, Put> call(String row) throws Exception {
						String rowKey = "saveKey" + row;
						Put put = new Put(Bytes.toBytes(rowKey));
						put.add(Bytes.toBytes("data"), Bytes.toBytes("q1"), Bytes.toBytes(row));

						return new Tuple2<ImmutableBytesWritable, Put>(
								new ImmutableBytesWritable(), put);
					}
				});

		hbasePuts.saveAsNewAPIHadoopDataset(conf2);
	}

}
