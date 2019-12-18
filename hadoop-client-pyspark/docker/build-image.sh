#!/bin/bash
echo "building hadoop-client-pyspark image..."

docker build --rm -t sidazhou/pyspark-notebook:sdhadoop .
