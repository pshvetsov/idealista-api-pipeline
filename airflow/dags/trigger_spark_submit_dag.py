import os
import sys
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import (
    ConsumeFromTopicOperator,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from src import create_or_update_spark_connection

logging.basicConfig(level=logging.INFO, filename='/opt/airflow/logs/trigger_spark_kob_dag.log', filemode='w', 
                    format='%(levelname)s:%(name)s:%(asctime)s:%(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('').addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger(__name__)

TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'real_estate_topic')


with DAG(
    'trigger_spark_job',
    start_date=datetime(2024, 1, 1, 00, 00),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:
    
    check_messages_in_topic_task = ConsumeFromTopicOperator(
        task_id = "check_messages_in_topic",
        kafka_config_id="kafka_default",
        topics=[TOPIC_NAME],
        max_batch_size = 1,
        max_messages = 1,
        poll_timeout = 5
    )
    create_spark_connection_task = PythonOperator(
        task_id = "create_spark_connection",
        python_callable = create_or_update_spark_connection
    )
    
    start_spark_job_task = TriggerDagRunOperator(
        trigger_dag_id="spark_submit_dag",
        task_id="spark_submit_dag_trigger",
        wait_for_completion=True,
        poke_interval=20,
    )
    
    completion_marker_task = DummyOperator(
        task_id="mark_completion"
    )
    
    check_messages_in_topic_task >> create_spark_connection_task \
        >> start_spark_job_task >> completion_marker_task