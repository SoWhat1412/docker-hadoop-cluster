#!/bin/bash

sudo docker rm -f hadoop-client-scala &> /dev/null
echo "start jupyter hadoop-client-scala container..."
sudo docker run -itd \
                --net=sdhadoop \
                -p 8888:8888 \
                --user root \
                -e GRANT_SUDO=yes \
                -v ~:/home/jovyan/work \
                --name hadoop-client-scala \
                --hostname hadoop-client-scala \
                sidazhou/all-spark-notebook:sdhadoop

# get into hadoop client container
sudo docker exec -it hadoop-client-scala bash
