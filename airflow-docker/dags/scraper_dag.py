from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from dotenv import load_dotenv
import json
import glob
import os
import logging

load_dotenv(override=True)
sys.path.append('/app/scraper/src/fetchers/reference_data/tradingview')
from data_coverage_scraper import countries_scraper, crawler_data_coverage 

sys.path.append('/app/scraper/src/database/reference_data')
from cassandra_client import CassandraClient

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def countries_tradingview_task_callable():
    data = countries_scraper()
    if data is None:
        logging.error("No data found!")
        return "No data found!"
    data_inserted = {
        'url': "https://www.tradingview.com/data-coverage/",
        'title': "Countries Data Coverage",
        'content': "Countries Data Coverage", 
        'data_crawled': json.dumps(data)}
    load_data_raw_to_cassandra_task_callable(data_inserted)
    return "Countries data scraped successfully!"

def exchanges_tradingview_task_callable():
    data = crawler_data_coverage()
    if data is None:
        logging.error("No data found!")
        return "No data found!"
    data_inserted = {
        'url': "https://www.tradingview.com/data-coverage/",
        'title': 'Exchanges Data Coverage',
        'content': 'Exchanges Data Coverage',
        'data_crawled': json.dumps(data)}
    load_data_raw_to_cassandra_task_callable(data_inserted)
    return "Exchanges data scraped successfully!"

def load_data_raw_to_cassandra_task_callable(data_crawled):
    client = CassandraClient()
    client.connect()
    client.create_keyspace()
    client.create_table_data_raw()
    client.insert_raw_data(data_crawled)
    client.close()
    return "Data loaded to Cassandra successfully!"

with DAG(
    "web_scraping_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    countries_task = PythonOperator(
        task_id="countries_task",
        python_callable=countries_tradingview_task_callable
    )
    exchanges_task = PythonOperator(
        task_id="exchanges_task",
        python_callable=exchanges_tradingview_task_callable
    )

    countries_task >> exchanges_task