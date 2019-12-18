FROM neo4j:latest

MAINTAINER liu jinjie <15232331412@126.com>

WORKDIR /var/lib/neo4j

COPY config/* /tmp/

RUN mv /tmp/load_data.cql /var/lib/neo4j/load_data.cql && \
    mv /tmp/reset_data_small.cql /var/lib/neo4j/reset_data_small.cql && \
    mv /tmp/reset_data_medium.cql /var/lib/neo4j/reset_data_medium.cql && \
    mv /tmp/neo4j.conf /var/lib/neo4j/conf/neo4j.conf

RUN cat /var/lib/neo4j/load_data.cql

RUN bin/neo4j-admin set-initial-password neo4j || true && \
    bin/neo4j start && sleep 10 && \
    cat /var/lib/neo4j/load_data.cql | NEO4J_USERNAME=neo4j NEO4J_PASSWORD=neo4j /var/lib/neo4j/bin/cypher-shell && \
    sleep 5 && bin/neo4j stop

RUN mv /tmp/docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["bash"]


# docker build --rm -t sidazhou/neo4j:sdhadoop .

# docker cp ./config/hive-site.xml hadoop-master:/usr/local/hive/conf/hive-site.xml
