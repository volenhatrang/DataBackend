from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging

sys.path.extend(
    [
        "/app/scraper/src/fetchers/reference_data/tradingview",
        "/app/scraper/src/database/reference_data",
    ]
)
# type: ignore
from holidays import * # noqa
import json
import pandas as pd
from datetime import datetime
from airflow.utils.task_group import TaskGroup  # Add this import

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models import Variable
from cassandra_client import CassandraClient # pylint: disable=import-error

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 17),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}
COUNTRIES_JSON_PATH = (
    "/app/scraper/src/database/reference_data/scraping_raw_json/tradingview"
)
year = datetime.now().year

def fetch_all_market_holiday(**kwargs):
    ti = kwargs["ti"]
    json_file_path = f"{COUNTRIES_JSON_PATH}/list_markets.json"
    try:
        with open(json_file_path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {json_file_path} not found!")
        exit(1)
    except json.JSONDecodeError:
        print(f"Error: File {json_file_path} is not a valid JSON!")
        exit(1)

    results = []
    for exchange in json_data["calendars"]:
        try:
            print(f"Processing {exchange}...")
            market_info = get_full_market_calendar(exchange, year=year)
            results.append({"exchange": exchange, "data": market_info})
        except Exception as e:
            print(f"Error processing {exchange}: {str(e)}")

    ti.xcom_push(
        key=f"fetch_market_holidays",
        value=results,
    )
    return results


def save_raw_data(**kwargs):
    ti = kwargs["ti"]
    cassandra_client = CassandraClient()
    cassandra_client.connect()
    cassandra_client.create_keyspace()
    data = ti.xcom_pull(
        key="fetch_market_holidays",
        task_ids='fetch_market_holidays',
    )
    if not data:
        logging.warning("No data")
        return "No data"
    cassandra_client.insert_raw_data(
        {
            "url": "pandas_market_calendars Library",
            "title": "Holiday of all markets",
            "crawl_date": datetime.now(),
            "content": "Get Holidays which is not trading",
            "data_crawled": json.dumps(data),
        }
    )
    logging.info(f"Data stored successfully for: {"Holidays"}")
    return f"Holidays processed!"


def save_holiday_table(**kwargs):
    ti = kwargs["ti"]
    cassandra_client = CassandraClient()
    cassandra_client.connect()
    cassandra_client.create_keyspace()
    data = ti.xcom_pull(
        key="fetch_market_holidays",
        task_ids='fetch_market_holidays',
    )
    if not data:
        logging.warning("No data")
        return "No data"   
     
    try:
        market_list = data
    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding failed: {e}")
        return "Invalid JSON data"

    for market_data in market_list:
        try:
            holiday_data = {
                "exchange": market_data.get("exchange"),
                "timezone": market_data["data"].get("timezone"),
                "open_time": market_data["data"]["regular_trading_hours"]["open_time"],
                "close_time": market_data["data"]["regular_trading_hours"]["close_time"],
                "regular_duration_hours": market_data["data"].get("regular_duration_hours"),
                "holidays": market_data["data"].get("holidays", []),
                "total_trading_days": market_data["data"].get("total_trading_days"),
                "period_start_date": market_data["data"]["period"]["start_date"],
                "period_end_date": market_data["data"]["period"]["end_date"],
            }

            cassandra_client.insert_holidays(holiday_data)
            logging.info(f"Inserted data for {holiday_data['exchange']}")

        except Exception as e:
            logging.error(f"Failed to process exchange {market_data.get('exchange')}: {e}")

    return f"Saved {len(market_list)} exchanges"

with DAG(
    f"trading_view_get_holiday_{year}",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    fetch_market_holidays_task = PythonOperator(
        task_id="fetch_market_holidays",
        python_callable=fetch_all_market_holiday,
    )
    
    save_raw_data_task = PythonOperator(
        task_id="save_raw_data",
        python_callable=save_raw_data,
    )

    save_holiday_table_task = PythonOperator(
        task_id="save_holiday_table",
        python_callable=save_holiday_table,
    )
    
    fetch_market_holidays_task >> [save_raw_data_task, save_holiday_table_task]
    
def cleanup():
    logging.info("Closing Cassandra connection.")
    cassandra_client.close()

dag.on_success_callback = cleanup
dag.on_failure_callback = cleanup
