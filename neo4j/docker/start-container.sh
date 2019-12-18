#!/bin/bash

# set -e

sudo docker rm -f hadoop-neo4j &> /dev/null
echo "start hadoop-neo4j container..."
sudo docker run -itd \
                --net=sdhadoop \
                -p 7474:7474 \
                -p 7687:7687 \
                --name hadoop-neo4j \
                --hostname hadoop-neo4j \
                sowhat/neo4j:sdhadoop

sudo docker exec -it hadoop-neo4j bash
