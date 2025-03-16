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
sys.path.extend([
    '/app/scraper/src/fetchers/reference_data/tradingview',
    '/app/scraper/src/database/reference_data'
])

from data_coverage_scraper import countries_scraper, crawler_data_coverage
from cassandra_client import CassandraClient

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Cassandra client singleton
cassandra_client = CassandraClient()
cassandra_client.connect()
cassandra_client.create_keyspace()


def fetch_and_store_data(fetch_function, title, url, content):
    logging.info(f"Scraping data for: {title}")
    data = fetch_function()
    if not data:
        logging.warning(f"No data found for: {title}")
        return f"No data for {title}"
    
    cassandra_client.insert_raw_data({
        'url': url,
        'title': title,
        'content': content,
        'data_crawled': json.dumps(data)
    })
    logging.info(f"Data stored successfully for: {title}")
    return f"{title} processed!"

def transform_data():
    logging.info("Starting data transformation")
    data = cassandra_client.fetch_latest_data_by_title()
    for item in data:
        title = item['title']
        data_loaded = json.loads(item['data_crawled'])
        
        if title == "Countries Data Coverage":
            for region_data in data_loaded:
                region = region_data.get('region', 'Unknown')
                for country in region_data.get('countries', []):
                    cassandra_client.insert_countries({
                        'region': region,
                        'country': country.get('country', 'Unknown'),
                        'data_market': country.get('data_market', None),
                        'country_flag': country.get('country_flag', None)
                    })
        elif title == "Exchanges Data Coverage":
            for exchange_item in data_loaded:
                for data in exchange_item.get('exchanges', []):
                    cassandra_client.insert_exchanges(data)
        else:
            logging.warning(f"Unknown title: {title}")
    logging.info("Data transformation complete")

with DAG("web_scraping_dag", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    countries_task = PythonOperator(
        task_id="scraping_raw_countries",
        python_callable=lambda: fetch_and_store_data(
            countries_scraper, "Countries Data Coverage",
            "https://www.tradingview.com/data-coverage/", "Countries Data Coverage")
    )

    exchanges_task = PythonOperator(
        task_id="scraping_raw_exchanges",
        python_callable=lambda: fetch_and_store_data(
            crawler_data_coverage, "Exchanges Data Coverage",
            "https://www.tradingview.com/data-coverage/", "Exchanges Data Coverage")
    )
    
    transform_task = PythonOperator(
        task_id="load_transformed_data",
        python_callable=transform_data
    )
    
    countries_task >> exchanges_task >> transform_task

def cleanup():
    logging.info("Closing Cassandra connection.")
    cassandra_client.close()

dag.on_success_callback = cleanup
dag.on_failure_callback = cleanup