# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import sys
# import os

# # /FinanceDataProject/FinanceDataScraper/data_fetcher/reference_data/tradingview
# sys.path.append('/opt/airflow/FinanceDataScraper/data_fetcher/reference_data/tradingview')
# from countries_scraper import countries_scraper as countries_scraper_main
# from exchanges_scraper import exchanges_scraper as exchanges_scraper_main


# with DAG(
#     dag_id = 'finance_scraper_dag',
#     start_date=datetime(2025,3,3),
#     schedule_interval= None,
#     catchup=False,
# ) as dag:
#     # task 1: run countries_scraper.py
#     scrape_countries = PythonOperator(
#         task_id='scrape_countries',
#         python_callable=countries_scraper_main,
#     )
#     # task 2 run exchanges_scraper.py
#     scrape_exchanges = PythonOperator(
#         task_id = 'scrape_exchanges',
#         python_callable=exchanges_scraper_main
#     )

# scrape_countries >> scrape_exchanges


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os


sys.path.append('/opt/airflow/FinanceDataScraper/data_fetcher/reference_data/tradingview')
from scraping import exchanges_scraper

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
        python_callable=exchanges_scraper
    )
