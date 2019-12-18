#!/bin/bash
echo "building neo4j image..."

docker build --rm -t sidazhou/neo4j:sdhadoop .
