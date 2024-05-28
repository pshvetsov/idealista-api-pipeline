import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from src import (pull_and_save_from_api, create_or_update_kafka_connection, 
                 send_to_kafka_topic)

logging.basicConfig(level=logging.INFO, filename='/opt/airflow/logs/main_dag.log', filemode='w', 
                    format='%(levelname)s:%(name)s:%(asctime)s:%(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('').addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger(__name__)

with DAG(
    'idealista_pipeline',
    start_date=datetime(2024, 1, 1, 00, 00),
    schedule_interval='@once',
    default_args={'retries':0, 
                  'retry_delay':timedelta(minutes=1)},
    catchup=False
) as dag:
    
    # Pull data from api
    pull_and_save_from_api_task = PythonOperator(
        task_id = "pull_and_save_from_api",
        python_callable = pull_and_save_from_api
    )
    
    # Create kafka connection, so Airflow sensor could listen to topic and 
    # trigger spark submit when a message arrives.
    create_or_update_kafka_connection_task = PythonOperator(
        task_id = "create_or_update_kafka_connection",
        python_callable=create_or_update_kafka_connection
    )
    
    # Launch listener sensor, which listens to kafka topic and as soon as a 
    # message arrives - will triger spark submit dag.
    trigger_trigger_task = TriggerDagRunOperator(
        task_id="trigger_trigger",
        trigger_dag_id="trigger_spark_job"
    )
    
    # Send data to kafka topic
    send_to_kafka_task = PythonOperator(
        task_id = "send_to_kafka",
        python_callable=send_to_kafka_topic
    )
    
    pull_and_save_from_api_task >> create_or_update_kafka_connection_task \
        >> trigger_trigger_task >> send_to_kafka_task
    

# Debug stuff.
if __name__ == "__main__":
    send_to_kafka_topic()


