#!/bin/bash

/home/mh/bin/spark-1.2.2-bin-hadoop2.4/bin/spark-submit --class com.blueskyarea.SaveDataToCouchbase --master local /home/mh/workspace/github/hbase/hbase/target/hbase-1.0-SNAPSHOT-jar-with-dependencies.jar $1 $2


