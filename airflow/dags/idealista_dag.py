import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from src import (pull_and_save_from_api, create_or_update_spark_connection, 
                 create_kafka_topic, send_to_kafka_topic)

logging.basicConfig(level=logging.INFO, filename='/opt/airflow/logs/idealista_api.log', filemode='w', 
                    format='%(levelname)s:%(name)s:%(asctime)s:%(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('').addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'shv',
    'start_date': datetime(2024, 1, 1, 00, 00),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'idealista-pipeline',
    default_args=default_args,
    schedule_interval='@once',
    description="Pull data from API, simulate flow, process with kafka and spark to cassandra",
    catchup=False
) as dag:
    
    # Create kafka topic, so submitted spark job can connect to kafka.
    create_kafka_topic_task = PythonOperator(
        task_id = "create_kafka_topic",
        python_callable=create_kafka_topic
    )
    
    # Establish airflow to spark connection.
    create_or_update_spark_connection_task = PythonOperator(
        task_id = "create_or_update_spark_connection",
        python_callable = create_or_update_spark_connection
    )
    
    # Submit spark job, that will run and process kafka messages 
    # as soon as any become available.
    spark_submit_task = SparkSubmitOperator(
        task_id = 'spark_submit',
        application = "/usr/local/spark/app/spark_main.py",
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        conn_id = "spark_default",
        dag = dag
    )
    
    # Pull data from api
    pull_and_save_from_api_task = PythonOperator(
        task_id = "pull_and_save_from_api",
        python_callable = pull_and_save_from_api
    )
    
    send_to_kafka_task = PythonOperator(
        task_id = "send_to_kafka",
        python_callable=send_to_kafka_topic
    )
    
    create_kafka_topic_task >> create_or_update_spark_connection_task >> \
        spark_submit_task >> pull_and_save_from_api_task >> send_to_kafka_task


if __name__ == "__main__":
    pull_and_save_from_api()
    send_to_kafka_topic()


