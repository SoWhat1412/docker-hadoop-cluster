FROM jupyter/all-spark-notebook

MAINTAINER liu jinjie <15232331412@126.com>

USER root
RUN id -u -n #user

RUN chown -R jovyan:100 ./.jupyter

# docker image already have  : openjdk version "1.8.0_191"
# bigdata have               : java version "1.8.0_111"
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=/usr/lib/jvm/java-8-openjdk-amd64:$PATH

# install hadoop 2.7.2
# https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.5/bk_release-notes/content/comp_versions.html
RUN wget https://github.com/kiwenlau/compile-hadoop/releases/download/2.7.2/hadoop-2.7.2.tar.gz && \
    tar -xzvf hadoop-2.7.2.tar.gz && \
    mv hadoop-2.7.2 /usr/local/hadoop && \
    rm hadoop-2.7.2.tar.gz

# set environment variable
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin


# scala 2.10.7
# https://downloads.lightbend.com/scala/2.10.7/scala-2.10.7.tgz
RUN wget https://downloads.lightbend.com/scala/2.10.7/scala-2.10.7.tgz && \
    tar -xzvf scala-2.10.7.tgz && \
    mv scala-2.10.7 /usr/local/scala && \
    rm scala-2.10.7.tgz
# set environment variable
ENV SCALA_HOME=/usr/local/scala
ENV PATH=$PATH:$SCALA_HOME/bin
# fixes stupid error on running scala
RUN touch $JAVA_HOME/release

# Apache Spark 1.6.1
# NOTE: this is for hadoop 2.6, NOT hadoop 2.7   spark-1.6.1-bin-hadoop2.7.tgz isn't a thing
# https://archive.apache.org/dist/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
RUN wget https://archive.apache.org/dist/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz && \
    tar -xzvf spark-1.6.1-bin-hadoop2.6.tgz && \
    mv spark-1.6.1-bin-hadoop2.6 /usr/local/spark && \
    rm spark-1.6.1-bin-hadoop2.6.tgz

RUN mv /usr/local/spark/spark-1.6.1-bin-hadoop2.6 ~ && \
    rm -r /usr/local/spark && \
    mv ~/spark-1.6.1-bin-hadoop2.6/ /usr/local/spark/

# set environment variable
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=/opt/conda/envs/python2/bin/python

# https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html#add-a-python-2-x-environment
# Create a Python 2.x environment using conda including at least the ipython kernel
# and the kernda utility. Add any additional packages you want available for use
# in a Python 2 notebook to the first line here (e.g., pandas, matplotlib, etc.)
RUN conda create --yes -p $CONDA_DIR/envs/python2 python=2.7 anaconda ipython ipykernel kernda && \
    conda clean -tipsy

# RUN echo "source activate python2" >> /home/jovyan/.bashrc
# ENV PATH /opt/conda/envs/python2/bin:$PATH

# # Create a global kernelspec in the image and modify it so that it properly activates
# # the python2 conda environment.
# RUN $CONDA_DIR/envs/python2/bin/python -m ipykernel install && \
# $CONDA_DIR/envs/python2/bin/kernda -o -y /usr/local/share/jupyter/kernels/python2/kernel.json
# #
# # Installed kernelspec python2 in /usr/local/share/jupyter/kernels/python2
# # {
# #   "display_name": "Python 2",
# #   "argv": [
# #     "bash",
# #     "-c",
# #     "source \"/opt/conda/bin/activate\" \"/opt/conda/envs/python2\" && exec /opt/conda/envs/python2/bin/python -m ipykernel_launcher -f '{connection_file}' "
# #   ],
# #   "language": "python"
# # }


# downgrading to toree 0.1.0 to support spark 1.6
RUN printf 'y\n' | jupyter kernelspec uninstall apache_toree_scala
RUN printf 'y\n' | /opt/conda/bin/python -m pip uninstall toree
# python3 because `which jupyter` is installed under python3.6

# https://github.com/jupyter/notebook/issues/3499
RUN conda update conda
RUN pip install --no-cache-dir toree==0.1.0
RUN jupyter toree install --sys-prefix --toree_opts='--nosparkcontext'
RUN rm -rf /home/$NB_USER/.local && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

# https://github.com/jupyter/notebook/issues/3499
# toree installs weird dependencies, need to upgraded back
RUN pip install jupyter -U

# https://github.com/jupyter/jupyter/issues/334
RUN conda install tornado=4.5.3

# config spark and yarn
# https://www.linode.com/docs/databases/hadoop/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# not used
RUN sed -i 's/export JAVA_HOME/# export JAVA_HOME/g' $HADOOP_HOME/etc/hadoop/hadoop-env.sh
# not used END

RUN conda install -c conda-forge jupyter_contrib_nbextensions

USER root

# setting timezone
RUN rm /etc/localtime && \
    ln -s /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

COPY config/* /tmp-sd/
COPY jars/* $SPARK_HOME/jars-sd/

RUN cp /tmp-sd/* $HADOOP_HOME/etc/hadoop/
RUN cp /tmp-sd/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf && \
    cp /tmp-sd/spark-env.sh $SPARK_HOME/conf/spark-env.sh

RUN mkdir -p /opt/apps && \
    ln -s /usr/lib/jvm/java-8-openjdk-amd64 /opt/apps/jdk1.8.0_111

RUN cp -pr /opt/conda/share/jupyter/kernels/apache_toree_scala/ /opt/conda/share/jupyter/kernels/apache_toree_scala_remote/
COPY kernel_remote.json /opt/conda/share/jupyter/kernels/apache_toree_scala_remote/kernel.json
COPY kernel_local.json /opt/conda/share/jupyter/kernels/apache_toree_scala/kernel.json

# https://stackoverflow.com/questions/40301891/spark-submit-to-yarn-as-a-another-user
ENV HADOOP_USER_NAME=kg


RUN mv /tmp-sd/docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["tini", "-g", "--"]
CMD ["docker-entrypoint.sh"]
#########################################################################
# USER jovyan



# put ADD or COPY last, they will always invalidate the build cache
COPY ./.jupyter /home/$NB_USER/.jupyter
COPY ./varInspector/var_list.py /opt/conda/share/jupyter/nbextensions/varInspector/var_list.py
COPY ./varInspector/main.js /opt/conda/share/jupyter/nbextensions/varInspector/main.js
COPY ./toc2.yaml /opt/conda/share/jupyter/nbextensions/toc2/toc2.yaml

RUN jupyter --version

#######################
# docker build --rm -t sidazhou/all-spark-notebook:sdhadoop .
