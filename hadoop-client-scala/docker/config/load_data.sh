#!/bin/bash
set -e

hadoop fs -mkdir -p /user/root/data
hadoop fs -put ~/data.csv hdfs:///user/root/data

hadoop fs -ls /user/root/data


# id_no, f1, f2, f3, label

hive <<'EOF'
DROP TABLE IF EXISTS data
;
CREATE EXTERNAL TABLE data(
    id_no string,
    f1 double,
    f2 double,
    f3 date,
    label double
    )
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'escapeChar' = '\\',
    'quoteChar' = '\"'
    )
STORED AS TEXTFILE
LOCATION
    'hdfs://hadoop-master:9000/user/root/data'
TBLPROPERTIES (
    'serialization.null.format' = '',
    'skip.header.line.count' = '1')
;
SELECT * from data
;
EOF

# set hive.execution.engine;
# SELECT COUNT(1) FROM data;

# INSERT INTO TABLE data values ('XXX','666','666','1999-12-31','6');
# CREATE TABLE students (name VARCHAR(64), age INT, gpa DECIMAL(3, 2))  CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC;
# INSERT INTO TABLE students VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
# select * from students;

# PARTITIONED BY (
    # f3 date)
