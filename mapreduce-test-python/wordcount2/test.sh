#!/bin/sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount2/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /wordcount2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /hdfs-test/mapreduce-test-data/test.txt /wordcount2/input/
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-file /hdfs-test/mapreduce-test-python/wordcount2/mapper.py -mapper /hdfs-test/mapreduce-test-python/wordcount2/mapper.py \
-file /hdfs-test/mapreduce-test-python/wordcount2/reducer.py -reducer /hdfs-test/mapreduce-test-python/wordcount2/reducer.py \
-input /wordcount2/input/* -output /wordcount2/output/
/usr/local/hadoop/bin/hdfs dfs -cat /wordcount2/output/part-00000
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount2/output/

