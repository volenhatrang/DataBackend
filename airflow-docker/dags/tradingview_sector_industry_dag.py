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

sys.path.extend(
    [
        "/app/scraper/src/fetchers/reference_data/tradingview",
        "/app/scraper/src/database/reference_data",
    ]
)

from sector_and_industries import *

# from app.scraper.src.fetchers.reference_data.tradingview.sector_and_industries import *
from cassandra_client import CassandraClient

T = TypeVar("T")


def chunk_list(lst: List[T], chunk_size: int) -> Iterable[List[T]]:
    """Split a list into chunks of specified size."""
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


@contextmanager
def timeout_handler():
    """Context manager to handle timeout and SIGTERM gracefully"""

    def signal_handler(signum, frame):
        if signum == signal.SIGTERM:
            logging.info("Received SIGTERM. Performing cleanup...")
            raise AirflowTaskTerminated("Task received SIGTERM signal")

    # Set up signal handler
    original_handler = signal.signal(signal.SIGTERM, signal_handler)
    try:
        yield
    finally:
        # Restore original handler
        signal.signal(signal.SIGTERM, original_handler)


# ==============================
# 🔹 Step 0: load list countries
# ==============================
# Function to get country list
COUNTRIES_JSON_PATH = "/app/scraper/src/database/reference_data/scraping_raw_json/tradingview/countries_with_flags_raw.json"
# COUNTRIES_JSON_PATH = "scraper/src/database/reference_data/scraping_raw_json/tradingview/countries_with_flags.json"


def clean_country_name(country: str) -> str:
    """Convert country name to valid task ID format"""
    return (
        country.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("(", "")
        .replace(")", "")
    )


def get_all_countries():
    with open(COUNTRIES_JSON_PATH, "r", encoding="utf-8") as file:
        data = json.load(file)

    all_countries = [
        country["country"].lower() for reg in data for country in reg["countries"]
    ]
    print(all_countries)
    return all_countries


all_countries = get_all_countries()

# ==============================
# 🔹 Step 1: get sectors and industries
# ==============================
datasets = ["sectors", "industries"]


def rate_limit(min_delay=1, max_delay=3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            time.sleep(random.uniform(min_delay, max_delay))
            return func(*args, **kwargs)

        return wrapper

    return decorator


@rate_limit(1, 3)
def fetch_tradingview_sector_and_industry_by_country(country, dataset, **context):
    """Fetch sector or industry data for a specific country."""
    with timeout_handler():
        try:
            cassandra_client = CassandraClient()  # Will return singleton instance
            country_source = country.lower().replace(" ", "-")

            table_name = f"tradingview_{dataset}"
            check_query = f"SELECT COUNT(*) as count FROM {table_name} WHERE country = %s ALLOW FILTERING"
            result = cassandra_client.query_data(check_query, (country_source,))
            existing_count = result[0].count if result else 0

            if existing_count > 0:
                logging.info(
                    f"Data for {country} in {dataset} already exists. Skipping..."
                )
                return f"Data for {country} in {dataset} already exists"

            if dataset == "sectors":
                df = get_tradingview_sectors_by_country(country=country_source)
            elif dataset == "industries":
                df = get_tradingview_industries_by_country(country=country_source)

            view_data(df)
            if df is None or df.empty:
                raise ValueError(
                    "❌ ERROR: df is None or empty! Check the data source."
                )

            cassandra_client.insert_df_to_cassandra(df, table_name)
            return f"Fetch successfully {dataset} of {country}!!!!"

        except AirflowTaskTerminated:
            logging.info(f"Task terminated for {country} - {dataset}")
            raise
        except Exception as e:
            logging.error(f"Error fetching {dataset} for {country}: {str(e)}")
            raise


def cleanup_cassandra_connection():
    """Cleanup Cassandra connection at the end of DAG run"""
    CassandraClient().close()
    logging.info("Cleaned up Cassandra connection")


# ==============================
# 🔹 Step 2: get components
# ==============================
# @task(execution_timeout=timedelta(minutes=30), do_xcom_push=False)
def fetch_tradingview_sector_and_industry_components_by_country(country, dataset):
    """Fetch components for a country's sectors or industries"""
    try:
        cassandra_client = CassandraClient()  # Will return singleton instance
        table_name = f"tradingview_{dataset}"
        query = f"SELECT * FROM {table_name} WHERE country = %s ALLOW FILTERING "
        components_df = cassandra_client.query_to_dataframe(query, (str(country),))

        if components_df.empty:
            print(f"⚠️ No data found for country: {country} in {dataset}. Skipping...")
            return f"No data for {country}, skipping..."

        view_data(components_df)
        components_links = components_df["component_url"].tolist()
        components_arr = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            components_arr = list(
                executor.map(
                    lambda link: (
                        print(f"Fetching components from {link}")
                        or time.sleep(random.randint(1, 10))
                        or get_tradingview_sectors_industries_components(link).assign(
                            component_url=link
                        )
                        if link
                        else pd.DataFrame()
                    ),
                    components_links,
                )
            )

        components_df = pd.concat(components_arr, ignore_index=True)
        view_data(components_df)

        table_name = f"tradingview_icb_components"
        cassandra_client.insert_df_to_cassandra(components_df, table_name)
        return f"fetch successfully {dataset} components of {country}!!!!"
    except Exception as e:
        logging.error(
            f"Error processing components for {country} - {dataset}: {str(e)}"
        )
        raise


def get_components_url_list(dataset, country):
    """Get component URLs for a country's dataset"""
    try:
        cassandra_client = CassandraClient()  # Will return singleton instance
        table_name = f"tradingview_{dataset}"
        query = f"SELECT * FROM {table_name} WHERE country = %s ALLOW FILTERING "
        components_df = cassandra_client.query_to_dataframe(query, (str(country),))

        if components_df.empty:
            print(f"⚠️ No data found for country: {country} in {dataset}. Skipping...")
            return f"No data for {country}, skipping..."

        view_data(components_df)
        return components_df["component_url"].tolist()
    except Exception as e:
        logging.error(
            f"Error getting component URLs for {country} - {dataset}: {str(e)}"
        )
        raise


def fetch_components_by_list(country, dataset, component_urls):
    """Fetch components for a subset of component_urls"""
    try:
        cassandra_client = CassandraClient()  # Will return singleton instance
        components_arr = []

        with ThreadPoolExecutor(max_workers=2) as executor:
            components_arr = list(
                executor.map(
                    lambda link: (
                        print(f"Fetching components from {link}")
                        or time.sleep(random.randint(1, 10))
                        or get_tradingview_sectors_industries_components(link).assign(
                            component_url=link
                        )
                        if link
                        else pd.DataFrame()
                    ),
                    component_urls,
                )
            )

        components_df = pd.concat(components_arr, ignore_index=True)
        view_data(components_df)

        if not components_df.empty:
            table_name = f"tradingview_icb_components"
            cassandra_client.insert_df_to_cassandra(components_df, table_name)

        return f"Fetched components for {len(component_urls)} links in {dataset} of {country}"
    except Exception as e:
        logging.error(
            f"Error processing component list for {country} - {dataset}: {str(e)}"
        )
        raise


# ==============================
# 🔹 Step 4: concat and save
# ==============================
def concat_and_save(**kwargs):
    ti = kwargs["ti"]
    all_sectors = []
    all_sectors_components = []
    all_industries = []
    all_industries_components = []

    for country in all_countries:
        country = clean_country_name(country)
        sector_data = ti.xcom_pull(
            task_ids=f"fetch_sectors_and_industries_tasks.fetch_{clean_country_name(country)}_sectors",
            key=f"{clean_country_name(country)}_{dataset}",
        )
        sector_component_data = ti.xcom_pull(
            task_ids=f"fetch_components_tasks.fetch_{clean_country_name(country)}_sectors_components",
            key=f"{clean_country_name(country)}_{dataset}_components",
        )
        industry_data = ti.xcom_pull(
            task_ids=f"fetch_sectors_and_industries_tasks.fetch_{clean_country_name(country)}_industries",
            key=f"{clean_country_name(country)}_{dataset}",
        )
        industry_component_data = ti.xcom_pull(
            task_ids=f"fetch_components_tasks.fetch_{clean_country_name(country)}_industries_components",
            key=f"{clean_country_name(country)}_{dataset}_components",
        )

        all_sectors.append(sector_data)
        all_sectors_components.append(sector_component_data)
        all_industries.append(industry_data)
        all_industries_components.append(industry_component_data)

    final_sectors = pd.concat(all_sectors, ignore_index=True)
    final_sectors_components = pd.concat(all_sectors_components, ignore_index=True)
    final_industries = pd.concat(all_industries, ignore_index=True)
    final_industries_components = pd.concat(
        all_industries_components, ignore_index=True
    )

    print(final_sectors)
    print(final_sectors_components)
    print(final_industries)
    print(final_industries_components)


with DAG(
    "trading_view_get_sectors_and_industries",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 2, 17),
        "retries": 5,  # Increase retries
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=120),  # Increase timeout to 2 hours
        "retry_exponential_backoff": True,  # Add exponential backoff
        "max_retry_delay": timedelta(minutes=60),
    },
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    max_active_tasks=8,
) as dag:

    start_task = DummyOperator(task_id="start")

    CHUNK_SIZE = 3
    country_chunks = chunk_list(all_countries, CHUNK_SIZE)

    list_countries_task = PythonOperator(
        task_id="get_list_countries",
        python_callable=get_all_countries,
    )

    chunk_groups = []

    for chunk_idx, country_chunk in enumerate(country_chunks):
        with TaskGroup(group_id=f"country_chunk_{chunk_idx}") as chunk_group:
            country_groups = {}

            for country in country_chunk:
                print(f"Process get sector and industry for {country}")
                with TaskGroup(group_id=clean_country_name(country.lower())) as tg:
                    for dataset in datasets:
                        with TaskGroup(
                            group_id=f"{clean_country_name(country.lower())}_{dataset}_group"
                        ) as sector_tg:
                            get_list_dataset_task = PythonOperator(
                                task_id=f"fetch_{clean_country_name(country)}_{dataset}_list",
                                python_callable=fetch_tradingview_sector_and_industry_by_country,
                                op_args=[country, dataset],
                                provide_context=True,
                                # Update retry configuration
                                retries=5,
                                retry_delay=timedelta(minutes=5),
                                execution_timeout=timedelta(minutes=120),
                                retry_exponential_backoff=True,
                                max_retry_delay=timedelta(minutes=60),
                                pool="default_pool",
                                pool_slots=1,
                            )
                    country_groups[clean_country_name(country.lower())] = tg

            chunk_groups.append(chunk_group)

    (
        start_task
        >> list_countries_task
        >> chunk_groups
        >> PythonOperator(
            task_id="cleanup_cassandra_connection",
            python_callable=cleanup_cassandra_connection,
            trigger_rule="all_done",  # Run this task regardless of upstream task status
        )
    )
