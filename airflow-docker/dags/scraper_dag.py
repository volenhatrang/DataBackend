from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os


sys.path.append('/opt/airflow/FinanceDataScraper/data_fetcher/reference_data/tradingview')
from exchanges_scraper import exchanges_scraper

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG("web_scraping_dag",
         default_args=default_args,
         schedule="@daily",
         catchup=False) as dag:
    scrape_task = PythonOperator(
        task_id="scrape_task",
        python_callable=exchanges_scraper(tradingview_path = "/opt/airflow/FinanaceDataScraper/database/reference_data/scraping_raw_json/tradingview")
    )
