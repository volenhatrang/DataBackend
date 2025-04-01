from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models import Variable, Pool
from airflow.exceptions import AirflowTaskTimeout, AirflowTaskTerminated
from airflow.models.baseoperator import chain
from typing import List, TypeVar, Iterable
from airflow.settings import Session

from datetime import datetime, timedelta
import sys
import os
import logging
import timeout_decorator
import time
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import random
import numpy as np
import signal
from contextlib import contextmanager
from pathlib import Path
import concurrent.futures
import math

sys.path.extend(
    [
        "/app/scraper/src/fetchers/reference_data/yahoo_finance",
        "/app/scraper/src/fetchers/reference_data/",
        "/app/scraper/src/database/reference_data/",
    ]
)

from yah_ohlc import *
from cassandra_client import CassandraClient


T = TypeVar("T")
LIST_CODES_FILE = "/app/scraper/src/database/reference_data/list_code_by_source/LIST_CODESOURCE_DOWNLOAD.txt"

# LIST_CODES_FILE = "scraper/src/database/reference_data/list_code_by_source/LIST_CODESOURCE_DOWNLOAD.txt"


@contextmanager
def timeout_handler():
    """Context manager to handle timeout and SIGTERM gracefully"""

    def signal_handler(signum, frame):
        if signum == signal.SIGTERM:
            logging.info("Received SIGTERM. Performing cleanup...")
            raise AirflowTaskTerminated("Task received SIGTERM signal")

    original_handler = signal.signal(signal.SIGTERM, signal_handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, original_handler)


EXPIRY_TIME = 60 * 60


def check_and_mark_group(group_id, reset=False):
    try:
        if reset:
            Variable.set("processed_groups", "{}")
            logging.info("🔄 Đã reset toàn bộ processed_groups!")
            return True
        processed_groups = Variable.get("processed_groups", default_var="{}")

        if not processed_groups.strip():
            logging.warning(
                "⚠️ Airflow Variable `processed_groups` is empty. Initializing..."
            )
            processed_groups = "{}"

        processed_groups = json.loads(processed_groups)

        current_time = time.time()

        processed_groups = {
            k: v for k, v in processed_groups.items() if current_time - v < EXPIRY_TIME
        }

        if group_id in processed_groups:
            logging.info(f"✅ Skipping {group_id}, already processed.")
            return False

        processed_groups[group_id] = current_time
        Variable.set("processed_groups", json.dumps(processed_groups))

        return True

    except json.JSONDecodeError as e:
        logging.error(f"❌ JSON Decode Error: {e}")
        Variable.set("processed_groups", "{}")
        return False


def cleanup_cassandra_connection():
    """Cleanup Cassandra connection at the end of DAG run"""
    CassandraClient().close()
    logging.info("Cleaned up Cassandra connection")


def check_updated_tickers():
    cassandra_client = CassandraClient()
    cassandra_client.connect()
    cassandra_client.create_keyspace()
    table_name = "download_yah_prices_day_history"
    updated_tickers = []
    today = datetime.now().date()
    today_str = today.strftime("%Y-%m-%d")
    logging.info(f"Checking tickers updated on {today_str}")

    today_start = datetime.combine(today, datetime.min.time()).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    # today_start_unix = int(time.mktime(today.timetuple()) * 1000)
    query = f"""
        SELECT ticker 
        FROM {table_name} 
        WHERE date = '{today_str}'
        AND updated >= '{today_start}' 
        AND source = 'YAH' 
        ALLOW FILTERING
    """

    try:
        rows = cassandra_client.query_data(query)
        updated_tickers = list(set(row.ticker for row in rows))
        logging.info(
            f"Found {len(updated_tickers)} tickers updated today: {updated_tickers[:5]}..."
        )
    except Exception as e:
        logging.error(f"Error querying Cassandra: {e}")
        updated_tickers = []

    cassandra_client.close()
    return updated_tickers


def get_list_tickers(nb_group=3, updated_tickers=None, source="YAH"):
    df = pd.read_csv(LIST_CODES_FILE, dtype=str)
    tickers = df[source].dropna().unique().tolist()

    if updated_tickers:
        tickers = [ticker for ticker in tickers if ticker not in updated_tickers]
        logging.info(f"After filtering, {len(tickers)} tickers remain to be fetched")

    total_tickers = len(tickers)
    if total_tickers == 0:
        logging.info("No tickers need to be fetched")
        return [[] for _ in range(nb_group)]

    chunk_size = math.ceil(total_tickers / nb_group)
    return [tickers[i : i + chunk_size] for i in range(0, total_tickers, chunk_size)]


def get_max_date_for_ticker(ticker):
    try:
        query = f"SELECT MAX(date) as max_date FROM {table_name} WHERE ticker = %s ALLOW FILTERING"
        result = cassandra_client.query(query, (ticker,))
        row = result.one()
        if row and row.max_date:
            return row.max_date
        return None
    except Exception as e:
        logging.error(f"❌ Error querying max date for {ticker}: {e}")
        return None


def calculate_optimal_period(max_date):
    current_date = datetime.now().date()

    if not max_date:
        return "max"

    if isinstance(max_date, datetime):
        max_date = max_date.date()

    days_diff = (current_date - max_date).days

    if days_diff <= 0:
        return None

    period_days = {
        "1d": 1,
        "5d": 5,
        "1mo": 30,
        "3mo": 90,
        "6mo": 180,
        "1y": 365,
        "2y": 730,
        "5y": 1825,
        "10y": 3650,
    }

    for period, days in period_days.items():
        if days_diff <= days:
            return period

    return "max"


def fetch_yah_prices_by_list(list_codesource, group_id, batch_size=50):
    if not check_and_mark_group(group_id, reset=True):
        return f"Skipped {group_id} as it was already processed."

    with timeout_handler():
        try:
            cassandra_client = CassandraClient()
            cassandra_client.connect()
            cassandra_client.create_keyspace()
            table_name = "download_yah_prices_day_history"
            all_data = []

            def fetch_single_ticker(ticker):
                retries = 3
                # max_date = get_max_date_for_ticker(ticker)
                query = f"SELECT MAX(date) as max_date FROM {table_name} WHERE ticker = %s ALLOW FILTERING"
                try:
                    result = cassandra_client.query_data(query, (ticker,))
                    if result and len(result) > 0:
                        row = result[0]
                        if row.max_date:
                            max_date = row.max_date
                            period = calculate_optimal_period(max_date)
                            logging.info(
                                f"Found max_date {max_date} for {ticker}, using period: {period}"
                            )
                        else:
                            period = "max"
                            logging.info(
                                f"No max_date found for {ticker}, using period: {period}"
                            )
                    else:
                        period = "max"
                        logging.info(f"No results for {ticker}, using period: {period}")
                except Exception as e:
                    logging.error(f"Error querying max date for {ticker}: {e}")
                    period = "max"

                for attempt in range(retries):
                    try:
                        df = download_yah_prices_by_code(ticker, period=period)
                        if df is None or df.empty:
                            return None
                        df["source"] = "YAH"
                        return df
                    except Exception as e:
                        if "429" in str(e):
                            time.sleep(5 * (attempt + 1))
                            continue
                        logging.error(f"❌ Error downloading {ticker}: {e}")
                        return None
                return None

            logging.info(
                f"Starting task {group_id} with {len(list_codesource)} tickers"
            )

            with ThreadPoolExecutor(max_workers=10) as executor:
                for i, ticker in enumerate(list_codesource):
                    result = fetch_single_ticker(ticker)

                    if result is not None:
                        result = result.reset_index()
                        all_data.append(result)

                    if len(all_data) >= batch_size or i == len(list_codesource) - 1:
                        if all_data:
                            common_columns = set.intersection(
                                *[set(df.columns) for df in all_data]
                            )
                            columns_to_keep = [
                                col
                                for col in [
                                    "ticker",
                                    "date",
                                    "open",
                                    "high",
                                    "low",
                                    "close",
                                    "volume",
                                    "source",
                                    "adjclose",
                                    "updated",
                                ]
                                if col in common_columns
                            ]
                            final_df = pd.concat(
                                [df[columns_to_keep] for df in all_data],
                                ignore_index=True,
                            )

                            final_df = final_df.dropna(subset=["date"])

                            cassandra_client.batch_insert_prices_async(
                                final_df, table_name, 100
                            )

                            logging.info(
                                f"Inserted batch of {len(final_df)} rows for {group_id}"
                            )

                            del final_df
                            all_data = []

            cleanup_cassandra_connection()
            return f"Completed {group_id} with {len(list_codesource)} tickers"

        except AirflowTaskTerminated:
            logging.info(f"Task terminated")
            raise
        except Exception as e:
            logging.error(f"Error fetching: {str(e)}")
            raise


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(days=365),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
}
NB_GROUPS = 5
with DAG(
    "download_yah_prices_day_history",
    default_args=default_args,
    description="Download price day of tickers from Yahoo Finance",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=3,
    max_active_tasks=3,
) as dag:

    start_task = DummyOperator(task_id="start")

    get_list_codesource_task = PythonOperator(
        task_id="get_list_codesource",
        python_callable=get_list_tickers,
        op_args=[NB_GROUPS, None, "YAH"],
    )

    list_updated_tickers = check_updated_tickers()

    list_codesource = get_list_tickers(
        nb_group=NB_GROUPS, updated_tickers=list_updated_tickers
    )

    fetch_tasks = []
    for i, chunk in enumerate(list_codesource):
        task = PythonOperator(
            task_id=f"fetch_yahoo_prices_chunk_{i}",
            python_callable=fetch_yah_prices_by_list,
            op_args=[chunk, f"group_{i}"],
        )
        fetch_tasks.append(task)

    end_task = DummyOperator(task_id="end")

    (
        start_task
        >> get_list_codesource_task
        >> fetch_tasks
        >> PythonOperator(
            task_id="cleanup_cassandra_connection",
            python_callable=cleanup_cassandra_connection,
            trigger_rule="all_done",
        )
        >> end_task
    )
