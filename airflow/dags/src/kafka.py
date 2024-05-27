import os
import time
import logging
import json
from airflow.models import Connection
from airflow.settings import Session
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from src import MinioConnection

logger = logging.getLogger(__name__)


def create_or_update_kafka_connection():
    """Programmatically create kafka connection inside airflow."""
    
    session = Session()
    conn_id = 'kafka_default'
    conn_type = 'Apache Kafka'
    extra = '{"bootstrap.servers": "kafka:9092", "group.id": "default-group"}'
    
    # Check if connection already exists
    conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if conn:
        logging.info("Airflow-Kafka connection already exists. Deleting...")
        session.delete(conn)
        session.commit()
        
    logging.info("Creating new Airflow-Kafka connection")
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        extra=extra
    )
    session.add(conn)
    session.commit()
    session.close()
    logging.info("Airflow-Kafka connection setup complete.")


def create_kafka_topic():
    """Create kafka topic :)"""
    
    TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'real_estate_topic')
    admin_client = AdminClient({"bootstrap.servers": "kafka:9092"})

    current_topics = admin_client.list_topics(timeout=10).topics
    if TOPIC_NAME in current_topics:
        logger.info(f"Kafka topic {TOPIC_NAME} already exists.")
    else:
        topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
        
        # Create the topic
        try:
            admin_client.create_topics([topic])
            logger.info(f"Kafka topic '{TOPIC_NAME}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create kafka topic '{TOPIC_NAME}': {e}")
        

def send_to_kafka_topic():
    """Imitate streaming to kafka topic."""
    
    create_kafka_topic()
    config = {
        'bootstrap.servers': 'kafka:9092',
    }
    TOPIC_NAME = os.environ.get('KAFKA_TOPIC')
    try:
        producer = Producer(**config)
        minio = MinioConnection()
        data_list, object_names = minio.read_unprocessed_data()
        for data, object_name in zip(data_list, object_names):
            try:
                for element in data['elementList']:
                    # Convert to JSON and send to topic
                    json_data = json.dumps(element).encode('utf-8')
                    producer.produce(TOPIC_NAME, json_data)
                    producer.flush()
                    
                    # Make short pause to simulate stream behavior.
                    time.sleep(2)
                
                # Save processed data. It is a lot of I/O operations, but i want
                # to be sure that data is updated as soon as it was sent.
                minio.write_object(
                    minio.METADATA_BUCKET_NAME,
                    minio.PROCESSED_FILES_HISTORY,
                    object_name,
                    append=True
                )
                logger.info(f"Data was successfully send. Object name {object_name}")
            except:
                logger.warn(f"Error occured while sending data to kafka. Object name {object_name}")
        
    except Exception as e:
        logger.warn(f"Failed to send message to kafka topic. Error {e}")