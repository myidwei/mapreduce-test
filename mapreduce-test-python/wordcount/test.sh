#!/bin/sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /wordcount/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /hdfs-test/mapreduce-test-data/test.txt /wordcount/input/
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-file /hdfs-test/mapreduce-test-python/wordcount/mapper.py -mapper /hdfs-test/mapreduce-test-python/wordcount/mapper.py \
-file /hdfs-test/mapreduce-test-python/wordcount/reducer.py -reducer /hdfs-test/mapreduce-test-python/wordcount/reducer.py \
-input /wordcount/input/* -output /wordcount/output/
/usr/local/hadoop/bin/hdfs dfs -cat /wordcount/output/part-00000
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount/output/

