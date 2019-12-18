FROM jupyter/all-spark-notebook

MAINTAINER liu jinjie <15232331412@126.com>

USER root
RUN id -u -n #user

COPY ./.jupyter /home/$NB_USER/.jupyter
RUN chown -R jovyan:100 ./.jupyter

# #########################################################################
# COPIED FROM sdhadoop projects dockerfiles
# install openssh-server, openjdk and wget
RUN apt-get update && apt-get install -y software-properties-common

RUN apt-get update && apt-get install -y openssh-server wget

COPY config/apt_config /etc/apt/preferences.d/debian

RUN apt install debian-archive-keyring && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 8B48AD6246925553 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 7638D0442B90D010 && \
    sudo add-apt-repository 'deb http://httpredir.debian.org/debian experimental main' && \
    sudo add-apt-repository 'deb http://httpredir.debian.org/debian sid main'
RUN apt-get update && apt-get install -y openjdk-7-jdk

# install hadoop 2.7.2
# https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.5/bk_release-notes/content/comp_versions.html
RUN wget https://github.com/kiwenlau/compile-hadoop/releases/download/2.7.2/hadoop-2.7.2.tar.gz && \
    tar -xzvf hadoop-2.7.2.tar.gz && \
    mv hadoop-2.7.2 /usr/local/hadoop && \
    rm hadoop-2.7.2.tar.gz

# set environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin

# ssh without key
RUN mkdir /home/jovyan/.ssh/ && \
    ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

RUN mkdir -p ~/hdfs/namenode && \
    mkdir -p ~/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs


# scala 2.11.12
# https://downloads.lightbend.com/scala/2.11.12/scala-2.11.12.tgz
RUN wget https://downloads.lightbend.com/scala/2.11.12/scala-2.11.12.tgz && \
    tar -xzvf scala-2.11.12.tgz && \
    mv scala-2.11.12 /usr/local/scala && \
    rm scala-2.11.12.tgz
# set environment variable
ENV SCALA_HOME=/usr/local/scala
ENV PATH=$PATH:$SCALA_HOME/bin
# fixes stupid error on running scala
RUN touch $JAVA_HOME/release


# Apache Spark 2.0.0
# https://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.7.tgz
RUN wget https://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.7.tgz && \
    tar -xzvf spark-2.0.0-bin-hadoop2.7.tgz && \
    mv spark-2.0.0-bin-hadoop2.7 /usr/local/spark && \
    rm spark-2.0.0-bin-hadoop2.7.tgz

RUN mv /usr/local/spark/spark-2.0.0-bin-hadoop2.7 ~ && \
    rm -r /usr/local/spark && \
    mv ~/spark-2.0.0-bin-hadoop2.7/ /usr/local/spark/


# set environment variable
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=/opt/conda/envs/py35/bin/python

# https://medium.com/@chadlagore/conda-environments-with-docker-82cdc9d25754
RUN conda create -n py35 python=3.5 anaconda
RUN echo "source activate py35" >> ~/.bashrc
ENV PATH /opt/conda/envs/py35/bin:$PATH
COPY config/spylon-kernel.json /opt/conda/share/jupyter/kernels/spylon-kernel/kernel.json


# Spylon-kernel
RUN pip install --upgrade pip && pip install --upgrade spylon-kernel
# RUN conda clean -tipsy
RUN python -m spylon_kernel install --sys-prefix
RUN rm -rf /home/$NB_USER/.local

# config spark and yarn
# https://www.linode.com/docs/databases/hadoop/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin


RUN sed -i 's/export JAVA_HOME/# export JAVA_HOME/g' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN rm /usr/bin/java && ln -s /usr/lib/jvm/java-7-openjdk-amd64/bin/java /usr/bin/java
ENV PATH=/usr/lib/jvm/java-7-openjdk-amd64:$PATH

RUN conda install -c conda-forge jupyter_contrib_nbextensions

USER root
# ENV HADOOP_HOME=/usr/local/hadoop
# RUN mkdir -p $HADOOP_HOME/etc/hadoop
# RUN mkdir -p /home/jovyan/.ssh/
# ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

COPY config/* /tmp/

RUN mv /tmp/ssh_config ~/.ssh/config && \
    mv /tmp/hadoop-env.sh $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml && \
    mv /tmp/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml && \
    mv /tmp/slaves $HADOOP_HOME/etc/hadoop/slaves && \
    mv /tmp/start-hadoop.sh ~/start-hadoop.sh && \
    mv /tmp/run-wordcount.sh ~/run-wordcount.sh && \
    mv /tmp/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf


# RUN chmod +x ~/start-hadoop.sh && \
#     chmod +x ~/run-wordcount.sh && \
#     chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
#     chmod +x $HADOOP_HOME/sbin/start-yarn.sh

RUN mv /tmp/docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["tini", "-g", "--"]
CMD ["docker-entrypoint.sh"]
#########################################################################
# USER jovyan



# put ADD or COPY last, they will always invalidate the build cache
COPY ./varInspector/var_list.py /opt/conda/share/jupyter/nbextensions/varInspector/var_list.py
COPY ./varInspector/main.js /opt/conda/share/jupyter/nbextensions/varInspector/main.js

RUN jupyter --version

#######################
# docker build --rm -t sidazhou/all-spark-notebook:sdhadoop .


















# # commands to run
# docker build --rm -t sidazhou/pyspark-notebook:latest -t sidazhou/pyspark-notebook:v2.0.0 .

# docker build --rm --no-cache -t sidazhou/pyspark-notebook:latest -t sidazhou/pyspark-notebook:v2.0.0 .

# docker run -d -p 8888:8888 --name jupyter -v $(pwd):/home/jovyan/work sidazhou/pyspark-notebook:latest
# # to run on linux box with security features, make sure the user `id -u` and group `id -g kg` are configured
# docker run -d -p 8888:8888 --name jupyter --user root -e GRANT_SUDO=yes -e NB_UID=1009 -e NB_GID=1009 -v ~:/home/jovyan/work sidazhou/pyspark-notebook:latest
# # but jupyter/pyspark-notebook doesnt work with NB_UID and NB_GID (on mac), so let's run following
# docker run -d -p 8888:8888 --name jupyter --user root -e GRANT_SUDO=yes -v ~:/home/jovyan/work sidazhou/pyspark-notebook:latest

# # to do some testing as root
# docker exec -it  --user root -e GRANT_SUDO=yes jupyter bash

# # to run ipynb detached
# docker exec -d jupyter nohup jupyter nbconvert --ExecutePreprocessor.timeout=-1 --execute --to notebook --inplace /home/jovyan/work/GEM/notebooks/0.1.2-hyperparam_opt_node2vec.ipynb

# docker rm $(docker ps -q -f status=exited)

# # additional notes
# https://github.com/docker/docker/issues/12193
# Add a way to "APPEND" to a file from within a Dockefile

# docker build --rm -f ./Dockerfile_py27 -t sidazhou/scipy-notebook-py27:latest .
