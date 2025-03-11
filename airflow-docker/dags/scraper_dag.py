from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
from dotenv import load_dotenv

load_dotenv(override=True)

# Add the path to sys.path for imports
# sys.path.append('/opt/airflow/FinanceDataScraper/data_fetcher/reference_data/tradingview')

# sys.path.append(os.getenv('FINANCE_SCRIPT_TRADINGVIEW_SCRAPER_PATH'))
# from data_coverage_scraper  import countries_scraper, crawler_data_coverage

from finance_data_scraper.data_fetcher.reference_data.tradingview.data_coverage_scraper import countries_scraper, crawler_data_coverage

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def countries_tradingview_task_callable():
    countries_scraper(tradingview_path=os.getenv('FINANCE_DATA_TRADINGVIEW_SCRAPER_PATH'))

def exchanges_tradingview_task_callable():
    crawler_data_coverage(tradingview_path=os.getenv('FINANCE_DATA_TRADINGVIEW_SCRAPER_PATH'))

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