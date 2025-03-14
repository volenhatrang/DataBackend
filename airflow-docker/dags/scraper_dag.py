from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import json
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)
sys.path.append('/app/scraper/src/fetchers/reference_data/tradingview')
sys.path.append('/app/scraper/src/database/reference_data')

from data_coverage_scraper import countries_scraper, crawler_data_coverage, setup_driver, scrape_tab
from cassandra_client import CassandraClient

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Cassandra client setup (singleton)
cassandra_client = CassandraClient()
cassandra_client.connect()
cassandra_client.create_keyspace()
cassandra_client.create_table_data_raw()

def fetch_and_store_data(fetch_function, title, url, content):
    logging.info(f"Starting scraping for: {title}")
    data = fetch_function()
    if not data:
        logging.error(f"No data found for: {title}")
        return f"No data found for: {title}"

    data_inserted = {
        'url': url,
        'title': title,
        'content': content,
        'data_crawled': json.dumps(data)
    }

    logging.info(f"Inserting data into Cassandra for: {title}")
    cassandra_client.insert_raw_data(data_inserted)
    return f"Data for {title} processed successfully!"

# Task callables
def countries_tradingview_task_callable():
    return fetch_and_store_data(
        fetch_function=countries_scraper,
        title="Countries Data Coverage",
        url="https://www.tradingview.com/data-coverage/",
        content="Countries Data Coverage"
    )

def exchanges_tradingview_task_callable():
    return fetch_and_store_data(
        fetch_function=crawler_data_coverage,
        title="Exchanges Data Coverage",
        url="https://www.tradingview.com/data-coverage/",
        content="Exchanges Data Coverage"
    )


def data_transformation():
    data = cassandra_client.fetch_latest_data_by_title()
    
    for item in data:
        title = item['title']
        if title == "Countries Data Coverage":
            data_loaded = json.loads(item['data_crawled']) 
            for region_data in data_loaded:
                region = region_data.get('region', 'Unknown')
                for country in region_data.get('countries', []):
                    country_data = {
                        'region': region,
                        'country': country.get('country', 'Unknown'),
                        'data_market': country.get('data_market', None),
                        'country_flag': country.get('country_flag', None)
                    }
                    cassandra_client.insert_countries(country_data) 
        elif title == "Exchanges Data Coverage":
            data_loaded = json.loads(item['data_crawled'])
            for exchange_item in data_loaded:
                exchange_data = exchange_item.get('exchanges', [])
                for data in exchange_data:
                    cassandra_client.insert_exchanges(data)
        else:
            print(f"Unknown title: {title}")


with DAG(
    "web_scraping_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    countries_task = PythonOperator(
        task_id="scraping_raw_countries",
        python_callable=countries_tradingview_task_callable
    )

    exchanges_task = PythonOperator(
        task_id="scraping_raw_exchanges",
        python_callable=exchanges_tradingview_task_callable
    )

    countries_task >> exchanges_task

def cleanup():
    logging.info("Closing Cassandra client connection.")
    cassandra_client.close()

dag.on_success_callback = cleanup
dag.on_failure_callback = cleanup
