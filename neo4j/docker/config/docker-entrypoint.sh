#!/bin/bash

set -e

neo4j start

# ./load_data.sh
exec "$@"
