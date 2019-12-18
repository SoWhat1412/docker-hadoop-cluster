#!/bin/bash

echo "sudo docker rm -f hadoop-client-scala"
sudo docker rm -f hadoop-client-scala &> /dev/null
echo "sudo rm -fr /data/tmp/tmp-docker/*"
sudo rm -fr /data/tmp/tmp-docker/*
# also make sure /tmp/docker have right permissions to be written to by docker
# eg try sudo chmod 777 /tmp/docker

echo "start jupyter hadoop-client-scala container..."
sudo docker run -itd \
                --net=host \
                --user root \
                -e GRANT_SUDO=yes \
                -e NB_UID=1011 -e NB_GID=1011 \
                -v ~:/home/jovyan/work \
                -v /data/tmp/tmp-docker:/tmp \
                --name hadoop-client-scala \
                sidazhou/scala-spark-notebook:sdhadoop

echo "monkey patching hosts file"
sudo docker exec -u 0 hadoop-client-scala /bin/sh -c "echo 10.100.12.12 bigdata-001 >> /etc/hosts && echo 10.100.12.31 bigdata-002 >> /etc/hosts"

# get into hadoop client container
sudo docker exec -it hadoop-client-scala bash


# 4040 spark.ui.port
# 8888 jupyter server
# spark.driver.blockManager.port     9990
# spark.broadcast.port               9991
# spark.driver.port                  9992
# spark.executor.port                9993
# spark.fileserver.port              9994
# spark.replClassServer.port         9995

# https://mapr.com/docs/61/Spark/PortsUsedbySpark.html
# Spark Standalone Master (RPC) 7077
# Spark Standalone Master (Web UI)  8580, 8980*
# Spark Standalone Worker 8581, 8981*
# Spark Thrift Server 2304
# Spark History Server  18080,18480*
# Spark External Shuffle Service (if yarn shuffle service is enabled) 7337
# CLDB  7222
# ZooKeeper 5181
# Nodes running ResourceManager 8032
# MapR Filesystem Server  5660, 5692
