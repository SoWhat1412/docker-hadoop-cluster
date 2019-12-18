#!/bin/bash
echo "building hadoop-cluster image..."

docker build --rm \
-t sidazhou/hadoop-cluster:sdhadoop .
