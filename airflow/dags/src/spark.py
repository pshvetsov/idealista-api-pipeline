import logging
from airflow.models import Connection
from airflow.settings import Session

logger = logging.getLogger(__name__)

def create_or_update_spark_connection():
    session = Session()
    conn_id = 'spark_default'
    conn_type = 'spark'
    host = 'spark://spark-master:7077'
    extra = '{"queue": "default"}'
    
    # Check if connection already exists
    conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if conn:
        logging.info("Airflow-spark connection already exists. Updating existing connection")
        conn.conn_type = conn_type
        conn.host = host
        conn.extra = extra
    else:
        logging.info("Creating new airflow-spark connection")
        conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            extra=extra
        )
        session.add(conn)
    
    session.commit()
    session.close()
    logging.info("Airflow-spark connection setup complete.")
