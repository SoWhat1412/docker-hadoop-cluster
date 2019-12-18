#!/bin/bash
echo "building image..."

docker build --rm \
-t sidazhou/scala-spark-notebook:sdhadoop .
