import os
import time
import logging
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from src import MinioConnection

logger = logging.getLogger(__name__)

def create_kafka_topic():
    topic_name = os.environ.get('KAFKA_TOPIC', 'real_estate_topic')
    admin_client = AdminClient({"bootstrap.servers": "kafka:9092"})

    current_topics = admin_client.list_topics(timeout=10).topics
    if topic_name in current_topics:
        logger.info(f"Kafka topic {topic_name} already exists.")
    else:
        topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        
        # Create the topic
        try:
            admin_client.create_topics([topic])
            logger.info(f"Kafka topic '{topic_name}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create kafka topic '{topic_name}': {e}")
        

def send_to_kafka_topic():
    config = {
        'bootstrap.servers': 'kafka:9092',
    }
    topic_name = os.environ.get('KAFKA_TOPIC')
    try:
        producer = Producer(**config)
        minio = MinioConnection()
        data_list, object_names = minio.read_unprocessed_data()
        for data, object_name in zip(data_list, object_names):
            try:
                for element in data['elementList']:
                    # Convert to JSON and send to topic
                    json_data = json.dumps(element).encode('utf-8')
                    producer.produce(topic_name, json_data)
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