import os
import sys
import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import (
    AwaitMessageTriggerFunctionSensor,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from src import create_or_update_spark_connection
import uuid

logging.basicConfig(level=logging.INFO, filename='/opt/airflow/logs/trigger_spark_kob_dag.log', filemode='w', 
                    format='%(levelname)s:%(name)s:%(asctime)s:%(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('').addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger(__name__)

TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'real_estate_topic')

def trigger_function(message):
    """Since we want to trigger the spark job as soon as a message arrives -
    we have no specific message criteria to be fulfilled. Simple check"""
    
    logger.info(f"Trigger message content: {message}")
    return message is not None
    

def submit_spark_job(message, **context):
    """A message was sent to Kafka topic and was read. Spark job can be started"""
    
    logger.info("Start submitting spark job...\nCreating airflow-spark connection...")
    create_or_update_spark_connection()
    
    logger.info("Triggering spark_submit_dag...")
    TriggerDagRunOperator(
        trigger_dag_id="spark_submit_dag",
        task_id="spark_submit_dag_trigger",
        wait_for_completion=True,
        poke_interval=20,
    ).execute(context)


with DAG(
    'trigger_spark_job',
    start_date=datetime(2024, 1, 1, 00, 00),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:
    trigger_spark_task = AwaitMessageTriggerFunctionSensor(
        task_id="trigger_spark",
        kafka_config_id="kafka_default",
        topics=[TOPIC_NAME],
        apply_function="trigger_spark_job_dag.trigger_function",
        event_triggered_function=submit_spark_job
    )
    
    trigger_spark_task