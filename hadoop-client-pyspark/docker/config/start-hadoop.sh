#!/bin/bash

echo -e "\n"

$HADOOP_HOME/sbin/start-dfs.sh

echo -e "\n"

$HADOOP_HOME/sbin/start-yarn.sh

echo -e "\n"

$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh --config $HADOOP_HOME/etc/hadoop start historyserver

echo -e "\n"

hdfs dfs -mkdir /spark-logs

echo -e "\n"

$SPARK_HOME/sbin/start-history-server.sh

echo -e "\n"
