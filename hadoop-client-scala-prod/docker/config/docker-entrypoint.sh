#!/bin/bash
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

set -e

# spark history server
mkdir /tmp/spark-events
chmod 777 /tmp/spark-events
/usr/local/spark/sbin/start-history-server.sh
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start my_first_process: $status"
  exit $status
fi



if [[ ! -z "${JUPYTERHUB_API_TOKEN}" ]]; then
  # launched by JupyterHub, use single-user entrypoint
  exec /usr/local/bin/start-singleuser.sh "$@"
elif [[ ! -z "${JUPYTER_ENABLE_LAB}" ]]; then
  . /usr/local/bin/start.sh jupyter lab "$@"
else
  . /usr/local/bin/start.sh jupyter notebook "$@"
fi
####### NOTHING RUNS BELOW THIS POINT #########
# because jupyter notebook running in foreground and not returning

# service ssh start

# service mysql stop
# chown -R mysql:mysql /var/lib/mysql /var/run/mysqld
# chmod 777 /var/run/mysqld
# service mysql start

# mysql -uroot <<'EOF'
# CREATE DATABASE metastore;
# USE metastore;
# CREATE USER 'hiveuser'@'%' IDENTIFIED BY 'hivepassword';
# GRANT all on *.* to 'hiveuser'@localhost identified by 'hivepassword';
# flush privileges;
# EOF

# echo "hep"
# schematool -initSchema -dbType mysql
# sed -i 's/export JAVA_HOME/# export JAVA_HOME/g' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

exec "$@"

# ./start-hadoop.sh
# ./load_data.sh


#################
# mysql -uroot -e "SHOW databases;"

# not needed for hive 2.1.0 it seems, but was needed for 2.0.0
# SOURCE /usr/local/hive/scripts/metastore/upgrade/mysql/hive-schema-0.14.0.mysql.sql;
