from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from dotenv import load_dotenv
import json
import glob

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
    return countries_scraper(tradingview_path='/data/tradingview_data')

def exchanges_tradingview_task_callable():
    return crawler_data_coverage(tradingview_path='/data/tradingview_data')

def load_to_cassandra_task_callable():
    client = CassandraClient()
    client.connect()
    path = '/data/tradingview_data'
    for json_file in glob.glob(f"{path}/*.json"):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if 'countries' in data[0]:  
                for region_data in data:
                    region = region_data['region']
                    for country in region_data['countries']:
                        client.insert_countries({'region': region, 'country': country, 'data_market': '', 'country_flag': ''})  
            else: 
                for exchange in data:
                    exchange_data = {
                        'exchange_name': exchange.get('exchange_name', ''),
                        'exchange_desc_name': exchange.get('exchange_desc_name', ''),
                        'country': exchange.get('country', ''),
                        'types': exchange.get('types', []),
                        'tab': exchange.get('tab', '')
                    }
                    client.insert_exchanges(exchange_data)  
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