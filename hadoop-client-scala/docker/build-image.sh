#!/bin/bash
echo "building hadoop-client-scala image..."

docker build --rm -t sidazhou/all-spark-notebook:sdhadoop .
