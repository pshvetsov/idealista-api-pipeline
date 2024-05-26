from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    'spark_submit_dag',
    start_date=datetime(2024, 1, 1, 00, 00),
    schedule_interval=None,
    catchup=False,
    render_template_as_native_obj=True
) as dag:
    
    # Submit spark job.
    spark_submit_task = SparkSubmitOperator(
        task_id = 'spark_submit',
        application = "/usr/local/spark/app/spark_main.py",
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        conn_id = "spark_default"
    )
    
    spark_submit_task

