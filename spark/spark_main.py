# export PYTHONPATH=${SPARK_HOME}/python/:$(echo ${SPARK_HOME}/python/lib/py4j-*-src.zip):${PYTHONPATH}
# 
import logging
import os
import sys
import time
import threading
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.functions import get_json_object, explode, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, MapType


logging.basicConfig(level=logging.INFO, filename='idealista_api.log', filemode='w', 
                    format='%(levelname)s:%(name)s:%(asctime)s:%(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('py4j').addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger('py4j')

class InactivityListener(StreamingQueryListener):
    """Define a listener class, which tracks stream inactivity"""
    
    def __init__(self, timeout, poll_interval):
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.last_activity_time = time.time()
        
    def onQueryStarted(self, event):
        logger.info(f"Query started: {event.id}")
        
    def onQueryProgress(self, event):
        logger.info(f"Query progress: {event.id}")
        self.last_activity_time = time.time()
        
    def onQueryTerminated(self, event):
        logger.info(f"Query terminated: {event.id}")
        
    def onQueryIdle(self, event):
        logger.info(f"Query idle: {event.id}")
        
        
def check_for_inactivity(listener):
    """ Check for last activity time and iterupts if it exceeds the threshold"""
    
    while True:
        time.sleep(listener.poll_interval)
        current_time = time.time()
        if (current_time - listener.last_activity_time) > listener.timeout:
            logger.info("No activity detected for defined period, stopping Spark stream.")
            
            # Stop the first active stream
            spark.streams.active[0].stop()
            break


def create_spark_session():
    """Create spark session.
    
    Define jar packages for kafka and cassandra. Define cassandra connection details.
    """
    
    # Create Spark entry point
    # Download packages using Maven coordinates (groupId:artifactId:version)
    logger.info("Creating spark session...")
    try:
        spark = SparkSession.builder\
            .appName("KafkaToCassandraProcessing") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
            .config("spark.cassandra.connection.host", "172.20.1.0") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username","cassandra")\
            .config("spark.cassandra.auth.password","cassandra")\
            .getOrCreate()
        logger.info("Spark connection created successfully.")
    except Exception as e:
        logger.error(f"Failed to create spark connection. Error {e}")
        
    return spark


def create_kafka_df(spark):
    """Create kafka streaming dataframe."""
    
    # Connect to kafka topic
    try:
        logger.info("Start initializing kafka connection ...")
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "real_estate_topic") \
            .option('startingOffsets', 'earliest') \
            .load()
        logger.info("Kafka connection created successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to kafka topic. Error {e}")
        
    return kafka_df
    
    
def process_kafka_df(kafka_df):
    """Perform structured kafka dataframe processing."""
    
    logger.info("Start processing kafka dataframe...")
    schema = StructType([
        StructField("address", StringType(), True),
        StructField("bathrooms", LongType(), True),
        StructField("country", StringType(), True),
        StructField("description", StringType(), True),
        StructField("detailedType", MapType(StringType(), StringType()), True),
        StructField("distance", StringType(), True),
        StructField("district", StringType(), True),
        StructField("exterior", BooleanType(), True),
        StructField("externalReference", StringType(), True),
        StructField("floor", StringType(), True),
        StructField("has360", BooleanType(), True),
        StructField("has3DTour", BooleanType(), True),
        StructField("hasLift", BooleanType(), True),
        StructField("hasPlan", BooleanType(), True),
        StructField("hasStaging", BooleanType(), True),
        StructField("hasVideo", BooleanType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("municipality", StringType(), True),
        StructField("newDevelopment", BooleanType(), True),
        StructField("numPhotos", LongType(), True),
        StructField("operation", StringType(), True),
        StructField("parkingSpace", MapType(StringType(), BooleanType()), True),
        StructField("price", DoubleType(), True),
        StructField("priceByArea", DoubleType(), True),
        StructField("priceInfo", MapType(StringType(), MapType(StringType(), DoubleType())), True),
        StructField("propertyCode", StringType(), True),
        StructField("propertyType", StringType(), True),
        StructField("province", StringType(), True),
        StructField("rooms", LongType(), True),
        StructField("showAddress", BooleanType(), True),
        StructField("size", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("suggestedTexts", MapType(StringType(), StringType()), True),
        StructField("thumbnail", StringType(), True),
        StructField("topNewDevelopment", BooleanType(), True),
        StructField("topPlus", BooleanType(), True),
        StructField("url", StringType(), True)
    ])

    transformed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")
        
    df = transformed_df.select(
        col("address"), 
        col("size"), 
        col("price"),
        col("pricebyarea"),
        col("rooms"), 
        col("bathrooms"),
        col("floor"),
        col("haslift"),
        col("propertytype"),
        col("parkingSpace.hasParkingSpace").alias("parkingspace"),
        col("exterior"),
        col("latitude"),
        col("longitude"),
        col("description"),
        col("detailedType.typology").alias("typology"), # complex
        col("url"),
        col("propertycode")
    )
    logger.info("Kafka dataframe processed successfully.")
    return df


# Function to test Cassandra connection
def test_cassandra_connection(spark):
    """Test if cassandra connection is available."""
    
    table_name = os.environ.get('CASSANDRA_TABLE')
    keyspace_name = os.environ.get('CASSANDRA_KEYSPACE')
    logger.info(f"Testing cassandra connection for\nTable: {table_name}\nKeyspace: {keyspace_name}")
    try:
        # Attempt to read a small amount of data from Cassandra
        df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table_name, keyspace=keyspace_name) \
            .load().limit(1)

        # If read is successful, log it
        logger.info("Successfully connected to Cassandra and read data.")
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")


def write_to_cassandra(writeDF, _):
    logger.info("Start writing to cassandra...")
    table_name = os.environ.get('CASSANDRA_TABLE')
    keyspace_name = os.environ.get('CASSANDRA_KEYSPACE')
    writeDF.write \
        .format("org.apache.spark.sql.cassandra")\
        .mode('append') \
        .option("keyspace", keyspace_name) \
        .option("table", table_name) \
        .save()
    
    
if __name__ == '__main__':
    logger.info("**** START SPARK JOB ****")
    spark = create_spark_session()
    
    # Add StreamListener, which will check for stream activity.
    listener = InactivityListener(timeout=30, poll_interval=10)
    spark.streams.addListener(listener)
    
    kafka_df = create_kafka_df(spark)
    df = process_kafka_df(kafka_df)
    
    # Call the function to test connection
    test_cassandra_connection(spark)

    logger.info("Printing df schema:")
    df.printSchema()
    
    # Before the processing starts, start listener on a separate thread.
    # It will listen to spark stream and will shut it down after defined 
    # inactivity time.
    inactivity_thread = threading.Thread(target=check_for_inactivity, args=(listener,))
    inactivity_thread.start()
    
    df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("append") \
        .start() \
        .awaitTermination()
    
    # Perform cleanup
    spark.streams.removeListener(listener)
    spark.stop()
    logger.info("Successfully written to cassandra.")

