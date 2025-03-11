FROM cassandra:4.0

COPY cassandra.yaml /etc/cassandra/cassandra.yaml
COPY init-cassandra.sh /docker-entrypoint-initdb.d/init-cassandra.sh
# COPY init-cassandra.cql /docker-entrypoint-initdb.d/init-cassandra.cql

RUN chmod +x /init-cassandra.sh

ENTRYPOINT [ "/init-cassandra.sh" ]
CMD ["cassandra", "-f"]
