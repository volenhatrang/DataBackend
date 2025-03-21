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

cassandra_client = CassandraClient()

def initialize_cassandra():
    """Khởi tạo kết nối và keyspace cho Cassandra nếu chưa có."""
    global cassandra_client
    if not cassandra_client.session: 
        cassandra_client.connect()
        cassandra_client.create_keyspace()
    logging.info("Cassandra client initialized.")

def fetch_and_store_data(fetch_function, title, url, content):
    """Lấy dữ liệu từ web và lưu vào Cassandra."""
    global cassandra_client
    initialize_cassandra()  
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
    """Biến đổi dữ liệu từ bảng web_crawl sang các bảng khác."""
    global cassandra_client
    initialize_cassandra() 
    logging.info("Starting data transformation")
    data = cassandra_client.fetch_latest_data_by_title()
    list_data = list(data.items())
    for item in list_data:
        title = item[0]
        data_loaded = json.loads(item[1]['data_crawled'])
        if title == "Countries Data Coverage":
            for region_data in data_loaded:
                region = region_data.get('region', 'Unknown')
                for country in region_data.get('countries', []):
                    data = {
                        'region': region,
                        'country': country.get('country'),
                        'data_market': country.get('data_market'),
                        'country_flag': country.get('country_flag')
                    }
                    cassandra_client.insert_countries(data)
        elif title == "Exchanges Data Coverage":
            for exchange_item in data_loaded:
                for data in exchange_item.get('exchanges', []):
                    cassandra_client.insert_exchanges(data)
        else:
            logging.warning(f"Unknown title: {title}")
    logging.info("Data transformation complete")

def cleanup():
    """Đóng kết nối Cassandra khi DAG hoàn thành hoặc thất bại."""
    global cassandra_client
    logging.info("Closing Cassandra connection.")
    cassandra_client.close()


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


dag.on_success_callback = cleanup
dag.on_failure_callback = cleanup