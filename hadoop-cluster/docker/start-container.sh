#!/bin/bash

# the default node number is 2
N=${1:-2}


# start hadoop master container
# 19888 mapreduce historyserver
sudo docker rm -f hadoop-master &> /dev/null
echo "start hadoop-master container..."
sudo docker run -itd \
                --net=sdhadoop \
                -p 4040:4040 \
                -p 8080:8080 \
                -p 8088:8088 \
                -p 18080:18080 \
                -p 19888:19888 \
                -p 50070:50070 \
                --name hadoop-master \
                --hostname hadoop-master \
                sidazhou/hadoop-cluster:sdhadoop

# 4040 default spark-ui when running spark application
# 8080 spark master ui (maybe not available if using yarn)
# 8088 yarn
# 9000 NameNode metadata service  Master Nodes (NameNode and any back-up NameNodes) 8020/9000
# 18080 spark history server (not set up)
# 19888 mr job history server
# 50070 hadoop overview

# start hadoop slave container
i=0
while [ $i -lt $N ]
do
	sudo docker rm -f hadoop-slave$i &> /dev/null
	echo "start hadoop-slave$i container..."
	sudo docker run -itd \
	                --net=sdhadoop \
	                --name hadoop-slave$i \
	                --hostname hadoop-slave$i \
	                sidazhou/hadoop-cluster:sdhadoop
	i=$(( $i + 1 ))
done

# get into hadoop master container
sudo docker exec -it hadoop-master bash
