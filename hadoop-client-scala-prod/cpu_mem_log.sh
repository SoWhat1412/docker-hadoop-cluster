#!/bin/bash
echo "Usage: nohup ./cpu_mem_log.sh PID &"
echo "Running: python2 cpu_mem_log.py $1 > cpu_mem_log.log"
echo "logging to cpu_mem_log.log"
echo "plot using cpu_mem_log_plot.ipynb"
echo "DISK(GB) hardcoded to /tmp/docker"

python2 cpu_mem_log.py $1 > cpu_mem_log.log
