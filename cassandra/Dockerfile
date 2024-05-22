FROM bitnami/cassandra:latest

ENV CASSANDRA_KEYSPACE real_estate_keyspace
ENV TABLE rent

ENTRYPOINT ["/opt/cassandra/cassandra_setup.sh"]
CMD ["cassandra", "-f"]