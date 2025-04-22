from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os
import sys

sys.path.extend(
    [
        "/app/scraper/src/fetchers/spark/",
    ]
)
with DAG(
    "spark_cassandra_job", start_date=datetime(2025, 1, 1), schedule_interval=None
) as dag:
    spark_task = SparkSubmitOperator(
        task_id="spark_cassandra_task",
        application="/app/scraper/src/spark/spark_test.py",
        conn_id="spark_connection",
        # master="spark://spark-master:7077",
        # jars="/opt/airflow/jars/spark-cassandra-connector_2.12-3.4.1.jar",
        # conf={
        #     "spark.cassandra.connection.host": "cassandra",
        #     "spark.cassandra.connection.port": "9042",
        # },
        total_executor_cores=4,
        executor_memory="2g",
    )
    spark_task
