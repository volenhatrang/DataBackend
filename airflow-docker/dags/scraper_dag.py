from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the path to sys.path for imports
sys.path.append('/opt/airflow/FinanceDataScraper/data_fetcher/reference_data/tradingview')

# Import your scraping function
from exchanges_scraper import exchanges_scraper

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 4),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Wrapper function to pass arguments to exchanges_scraper
def scrape_task_callable():
    tradingview_path = "/opt/airflow/FinanceDataScraper/database/reference_data/scraping_raw_json/tradingview"
    exchanges_scraper(tradingview_path=tradingview_path)

# Define the DAG
with DAG(
    "web_scraping_dag",
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual execution
    catchup=False
) as dag:
    
    # Define the scraping task
    scrape_task = PythonOperator(
        task_id="scrape_task",
        python_callable=scrape_task_callable  # Pass the wrapper function
    )
