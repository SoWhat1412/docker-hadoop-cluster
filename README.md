0) General notes
=============
`sowhat/hadoop:sdhadoop` is based on `docker pull kiwenlau/hadoop:1.0`

Each folder `./*/docker/config` are the same, so that hadoop-master, hadoop-slave and hadoop-client all have same setup

Each folder in `./*/docker` contains `build-image.sh` and `start-container.sh`

`./*/docker/config` is copied to `/tmp` inside container and then moved to appropriate places in Dockerfile

Spark uses `$HADOOP_CONF_DIR` to find yarn when `--master yarn` is set

spark-neo4j-connector finds neo4j by setting `spark.neo4j.bolt.url` in `$SPARK_HOME/conf/spark-defaults.conf`

Use `http://localhost:8088` to see hadoop status
Use `yarn logs -applicationId <app_id>` to see hadoop logs
Use `docker log -f <container_name>` to see container logs

0.1) Versions
-------------
Referencing https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.5/bk_release-notes/content/comp_versions.html

java 1.7
scala 2.11.12
python 3.5 (installation in Dockerfile is very dodgy)
hadoop 2.7.2
hive 2.3.4
spark 2.0.0
neo4j 3.5.3

1) hadoop-cluster
================
Before starting the container, we need our own bridge network

    docker network create --driver=bridge sdhadoop

Set cluster size (Optional), build the image, and then start the container (master + 2x slaves)

    cd hadoop-cluster/docker
    ./resize-cluster.sh 2
    ./build-image.sh
    ./start-container.sh 2

Next, inside the container, start cluster and check everything works

    ./start-hadoop.sh
    ./run-wordcount.sh

check out

    http://localhost:8088
    http://localhost:19888
    http://localhost:50070
    http://localhost:18080
    http://localhost:4040 (should work, but doesn't work)

TODO: mount the hdfs data folder, so that data persist accross containers

1.1) Hive
---------
Hive is also installed, but the data is not persisted. Ie dhfs is not mounted, all data is lost when container is deleted.
hive data is created inside the container temporarily using

    ./load_data_hive.sh

which `loads data.csv` into hive/hdfs

TODO: move metadb server (mysql) for hive to another container

1.2) Spark
----------
`--master yarn` is set by default

Test in container using

    spark-submit --master yarn --deploy-mode client \
    --class org.apache.spark.examples.SparkPi \
    $SPARK_HOME/examples/jars/spark-examples_2.11-2.0.0.jar 10


2) hadoop-client-pyspark
=========================
Image is built, essentially, by starting from jupyter/pyspark-notebook, then
change the python version (very dodgy), since we needed python3.5 for spark 2.0.0, then
concatenating hadoop-cluster Dockerfile after it, to make all versions the same as hadoop-cluster
(this was done before full understanding of how to use jupyter/pyspark-notebook properly)

    cd hadoop-client-pyspark/docker
    ./build-image.sh
    ./start-container.sh

Test in container using

    spark-submit --master yarn --deploy-mode client \
    --class org.apache.spark.examples.SparkPi \
    $SPARK_HOME/examples/jars/spark-examples_2.11-2.0.0.jar 10

Checkout

    http://localhost:8889


Test in jupyter (python3 kernel) using

    import findspark
    findspark.init()
    import pyspark
    import random

    conftfos = pyspark.SparkConf().setAll([('spark.executor.cores','1')])
    sc = pyspark.SparkContext(appName="Pi", conf=conftfos)

    def inside(p):
      x, y = random.random(), random.random()
      return x*x + y*y < 1

    num_samples = 1000
    count = sc.parallelize(range(0, num_samples)).filter(inside).count()
    pi = 4. * count / num_samples
    print(pi)


3) hadoop-neo4j
===============
Just normal neo4j, nothing to do with hadoop, from official neo4j:latest. Added initial data, see `load_data.cql`.
Rerun `build-image.sh` if fail, since method of adding initial data is dodgy.

    cd neo4j/docker
    ./build-image.sh
    ./start-container.sh

To reset data, use one of the following (inside container):

    cat /var/lib/neo4j/reset_data_small.cql | NEO4J_USERNAME=neo4j NEO4J_PASSWORD=neo4j /var/lib/neo4j/bin/cypher-shell
    cat /var/lib/neo4j/reset_data_medium.cql | NEO4J_USERNAME=neo4j NEO4J_PASSWORD=neo4j /var/lib/neo4j/bin/cypher-shell

Checkout

    http://localhost:7474
    (bolt://localhost:7687)

TODO: mount the neo4j data folder, so that data persist accross containers


4) hadoop-client-scala
======================
I wanted to stay with python (https://stackoverflow.com/questions/48169520/neo4j-as-data-source-for-pyspark)
but python graphframe can't import from neo4j (and Mazerunner is way out of date)
so I have to use scala and neo4j-spark-connector, using scala also allows usage of graphx, scala+spark has better support/docs
Note: Another idea of importing data (hence allowing pyspark) is to process hdfs csv files before entering neo4j, some people were using similar architectures. But too much work.

The image is from jupyter/all-spark-notebook, the challenge was to get scala working inside jupyter.
Tried toree briefly, couldn't get toree 0.3.x working with java 1.7
I got spylon working, see `spylong-kernel.json` and Dockerfile for dodgy python installations and paths.

hadoop-client-scala/**/Dockerfile should be almost the same as hadoop-client-pyspark/**/Dockerfile, although it doesnt look like it

    cd hadoop-client-scala/docker
    ./build-image.sh
    ./start-container.sh

Test in container using

    spark-submit --master yarn --deploy-mode client \
    --class org.apache.spark.examples.SparkPi \
    $SPARK_HOME/examples/jars/spark-examples_2.11-2.0.0.jar 10

Checkout

    http://localhost:8888

Test in jupyter (spylon) using

    println(sc.master)
    val rdd = sc.parallelize(0 to 999)
    rdd.takeSample(false, 5)

4.1) neo4j-spark-connector for spark 2.0.0
------------------------------------------
For neo4j-spark-connector, we use 2.1.0-M4 (latest version compitible with java 1.7), and requires neo4j 3.x
see `hadoop-client-scala/13_spylon_neo4j_graphx_diff_label.ipynb` for more notes.
As mentioned above, neo4j-spark-connector doesnt support python

There is a big problem with neo4j-spark-connector when saving back to neo4j with `saveGraph()`, the connector duplicates nodes, since it can't find the right nodes to write to
Extremely ugly work around was found if we add temporary label `touchedBySpark` on all nodes, and add temporary `touchedBySpark_rel` relationship to all relationships
The problem then becomes temporary labels and relations are hard to delete, and takes up good amount of space

see `hadoop-client-scala/13_spylon_neo4j_graphx_diff_label.ipynb` for more notes.



5) hadoop-client-scala-prod
===========================
Note, bigdata refers to companies bigdata cluster; 10.100.34.19 is remote server (behind access gateway), connected to bigdata cluster.
We log into 10.100.34.19, and clone this folder into it.

Now we try to set up a jupyter notebook (scala) that can talk to bigdata cluster.
Task is mainly to find right configuration and right versions of everything.

Hadoop configs in docker/config are copied from bigdata-009, which is the `$HADOOP_CONF_DIR` inside the container.
`$HADOOP_USER_NAME` is set to `kg`, and proper queue is used `--queue kg`.
Monkey patching hosts file for bigdata-* on docker run (proper way is to find how to do this on docker build).

    cd <project_folder>
    ./build-image.sh
    ./start-container.sh

See `start-container.sh` for notes on ports

test in container using

    spark-submit --class org.apache.spark.examples.SparkPi \
    --master local[*] \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    --queue kg \
    $SPARK_HOME/lib/spark-examples*.jar \
    10

5.1) Versions on the bigdata cluster
------------------------------------
java 1.8
scala 2.10.7
python 3.6.8
hadoop 2.7.x (???)
hive 2.0.0
spark 1.6.1
neo4j 3.3.0 (test db)
neo4j 3.x (???) (prod db)

spark 1.6 doesnt support `spark.driver.bindAddress`, meaning that bigdata cant find return address of docker, hence had to change docker network to host, instead of bridge.

5.2) jupyter scala kernel
-------------------------
In the end, got Toree working, since now using java 1.8
Spylon wouldnt work as it requires Spark 2.0, Scala 2.11 (all releases)

https://www.youtube.com/watch?v=nRG8FdSbPhA
downgraded to Toree 0.1.x which supports spark 1.6.x
(Toree later versions are for spark 2.x)

two jupyter kernels are set up:
`kernel_local.json`, connecting to local[20] (ie 10.100.34.19)
and `kernel_remote.json`, connecting to remote (ie bigdata cluster)

these files shouldnt require any changes, all config should happen in spark-defaults.conf
if change is needed, then specify changes in SPARK_OPTS, which overrides spark-defaults.conf

5.3) Adding packages manually
-----------------------------
Normally, we would add packages automatically, `spark-shell --packages neo4j-contrib:neo4j-spark-connector:1.0.0-RC1`
but the problems are: installation is lost when container is destroyed; network issues, download from some source are forbidden

1) First, we can run `spark-shell --packages neo4j-contrib:neo4j-spark-connector:1.0.0-RC1` locally (anywhere internet isnt blocked) and observe which jar files it tries to download, and then wget these files manually to `docker/jars` folder
2) Then, change `spark.jars` and `spark.driver.extraClassPath` the file `spark-defaults.conf`, see `spark-defaults.conf` for examples
3) Then, rebuild the image, and restart container

TODO: check if `spark.driver.extraClassPath` is really needed to be set


5.4) neo4j-spark-connector
--------------------------
downgraded to neo4j-spark-connector:1.0.0-RC1 which supports spark 1.6
(neo4j-spark-connector later versions are for spark 2.x)

TODO: APIs are substantially different, need further investigation
and `saveGraph()` problem probably still exist

Since there are two kernels, local and remote (bigdata yarn), then we want to make sure everything works in following grid

|        | shell | jupyter |
|--------|-------|---------|
| local  |       |         |
| remote |       |         |

For notes to test everything works, see
haddop-client-scala-prod/*01*.ipynb
haddop-client-scala-prod/*02*.ipynb


5.5) problem with neo4j-spark-connector:1.0.0-RC1
--------------------------
Using 1.0.0-RC1 have a problem with `Neo4j Session object leaked`, which was suspected to be fixed in later neo4j-spark-connector versions
However, later neo4j-spark-connector versions were written for spark 2.x, but gladly I found a fork on github, which fixed the problem,
https://github.com/Gfeuillen/neo4j-spark-connector
See hadoop-client-scala-prod/neo4j-spark-connector-full-2.0.0-M2-s_1.6.1/README.md for building the jar
Then we put the jar in docker/jars and change spark-defaults.conf, and it works

See more notes in hadoop-client-scala-prod/local03_load_medium_data.ipynb










