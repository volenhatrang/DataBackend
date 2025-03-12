from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from dotenv import load_dotenv
import json
import glob

load_dotenv(override=True)

sys.path.append(os.getenv('FINANCE_SCRIPT_ROOT_PATH', '/app/scraper/src'))
from fetchers.tradingview.data_coverage_scraper import countries_scraper, crawler_data_coverage
from database.reference_data.cassandra_client import CassandraClient


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def countries_tradingview_task_callable():
    return countries_scraper(tradingview_path=os.getenv('FINANCE_DATA_TRADINGVIEW_SCRAPER_PATH', '/data/tradingview_data'))

def exchanges_tradingview_task_callable():
    return crawler_data_coverage(tradingview_path=os.getenv('FINANCE_DATA_TRADINGVIEW_SCRAPER_PATH', '/data/tradingview_data'))

def load_to_cassandra_task_callable():
    client = CassandraClient()
    client.connect()
    path = os.getenv('FINANCE_DATA_TRADINGVIEW_SCRAPER_PATH', '/data/tradingview_data')
    for json_file in glob.glob(f"{path}/*.json"):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if 'countries' in data[0]:  
                for region_data in data:
                    region = region_data['region']
                    for country in region_data['countries']:
                        client.insert_country(region, country)
            else: 
                for exchange in data:
                    client.insert_exchange(exchange)
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
    load_to_cassandra_task = PythonOperator(
        task_id="load_to_cassandra",
        python_callable=load_to_cassandra_task_callable
    )
    countries_task >> exchanges_task >> load_to_cassandra_task