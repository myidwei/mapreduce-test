#!/bin/sh
../../start.sh
mvn clean package
/usr/local/hadoop/bin/hdfs dfs -rm -r /logstat3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /logstat3/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /logstat3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../mapreduce-test-data/access.log /logstat3/input/
/usr/local/hadoop/bin/hadoop jar target/mapreduce-test-logstat3-1.0-SNAPSHOT.jar hadoop.mapreduce.test.logstat3.RunJob
for i in $(seq 0 23)
do
if [ $i -gt 9 ] ; then
/usr/local/hadoop/bin/hdfs dfs -cat /logstat3/output/part-r-000$i | head -n 10
fi
if [ $i -lt 10 ] ; then
/usr/local/hadoop/bin/hdfs dfs -cat /logstat3/output/part-r-0000$i | head -n 10
fi
done
/usr/local/hadoop/bin/hdfs dfs -rm -r /logstat3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /logstat3/output/
mvn clean
../../stop.sh
