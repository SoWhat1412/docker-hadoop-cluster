#!/bin/bash

sudo docker rm -f hadoop-client-pyspark &> /dev/null
echo "start jupyter hadoop-client-pyspark container..."
sudo docker run -itd \
                --net=sdhadoop \
                -p 8889:8888 \
                --user root \
                -e GRANT_SUDO=yes \
                -v ~:/home/jovyan/work \
                --name hadoop-client-pyspark \
                --hostname hadoop-client-pyspark \
                sidazhou/pyspark-notebook:sdhadoop

# get into hadoop client container
sudo docker exec -it hadoop-client-pyspark bash
