#!/usr/bin/env bash

# Define the Cassandra service host and port
CASSANDRA_HOST=172.20.1.0
CASSANDRA_PORT=9042

KEYSPACE=${CASSANDRA_KEYSPACE:-real_estate_keyspace}
TABLE_NAME=${CASSANDRA_TABLE:-rent_data}

# until printf "" 2>>/dev/null >>/dev/tcp/$CASSANDRA_HOST/$CASSANDRA_PORT; do
    # sleep 5;
    # echo "Waiting for cassandra...";
# done
# Continuously check if Cassandra is ready and can process commands
until cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -u $CASSANDRA_USERNAME -p $CASSANDRA_PASSWORD -e "DESCRIBE KEYSPACES;" &> /dev/null; do
    echo "Waiting for Cassandra to be ready..."
    sleep 5
done

echo "Describing keyspaces..."
echo "Username: ${CASSANDRA_USERNAME}"
echo "Password: ${CASSANDRA_PASSWORD}"
echo "Keyspace: ${KEYSPACE}"
echo "Table name: ${TABLE_NAME}"

cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -u $CASSANDRA_USERNAME -p $CASSANDRA_PASSWORD -e "DESCRIBE KEYSPACES;"

echo "Cassandra is up - executing schema setup"

# Run CQL commands to create keyspace and table
cqlsh $CASSANDRA_HOST $CASSANDRA_PORT -u $CASSANDRA_USERNAME -p $CASSANDRA_PASSWORD -e "
CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
};

USE $KEYSPACE;

CREATE TABLE IF NOT EXISTS $TABLE_NAME (
  propertycode text PRIMARY KEY,
  address text,
  size double,
  price double,
  pricebyarea double,
  rooms int,
  bathrooms int,
  floor text,
  haslift boolean,
  propertytype text,
  parkingspace boolean,
  exterior boolean,
  latitude double,
  longitude double,
  description text,
  typology text,
  url text
);"

echo "Cassandra schema setup completed."