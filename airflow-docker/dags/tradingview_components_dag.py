from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import sys
import os
import logging
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import random
import json

sys.path.extend(
    [
        "/app/scraper/src/fetchers/reference_data/tradingview",
        "/app/scraper/src/database/reference_data",
    ]
)

from sector_and_industries import *
from cassandra_client import CassandraClient

# Constants
COUNTRIES_JSON_PATH = "/app/scraper/src/database/reference_data/scraping_raw_json/tradingview/countries_with_flags_raw.json"
datasets = ["sectors", "industries"]


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
    """Get list of all countries from JSON file"""
    with open(COUNTRIES_JSON_PATH, "r", encoding="utf-8") as file:
        data = json.load(file)

    all_countries = [
        country["country"].lower() for reg in data for country in reg["countries"]
    ]
    logging.info(f"Loaded {len(all_countries)} countries from JSON file")
    return all_countries


def rate_limit(min_delay=1, max_delay=3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            time.sleep(random.uniform(min_delay, max_delay))
            return func(*args, **kwargs)

        return wrapper

    return decorator


@rate_limit(1, 3)
def get_components_for_country(country: str, dataset: str, **context):
    """Get components for a specific country and dataset (sectors/industries)"""
    try:
        cassandra_client = CassandraClient()

        # Check if components for this country already exist
        check_query = "SELECT COUNT(*) as count FROM tradingview_icb_components WHERE country = %s ALLOW FILTERING"
        result = cassandra_client.query_data(check_query, (str(country),))
        existing_count = result[0].count if result else 0

        if existing_count > 0:
            logging.info(
                f"Components for {country} already exist in tradingview_icb_components. Skipping..."
            )
            return f"Components for {country} already exist"

        # Get component URLs from sectors/industries table
        table_name = f"tradingview_{dataset}"
        query = f"SELECT * FROM {table_name} WHERE country = %s ALLOW FILTERING"
        components_df = cassandra_client.query_to_dataframe(query, (str(country),))

        if components_df.empty:
            logging.warning(f"⚠️ No {dataset} found for country: {country}")
            return f"No {dataset} data for {country}"

        # Get all component URLs and filter out None/empty values
        components_links = [
            url for url in components_df["component_url"].tolist() if url
        ]

        if not components_links:
            logging.warning(f"⚠️ No valid component URLs found for {country} {dataset}")
            return f"No valid component URLs for {country} {dataset}"

        logging.info(f"Found {len(components_links)} {dataset} for {country}")

        # Process components in batches to avoid overload
        BATCH_SIZE = 5
        components_arr = []
        failed_links = []

        for i in range(0, len(components_links), BATCH_SIZE):
            batch = components_links[i : i + BATCH_SIZE]
            with ThreadPoolExecutor(max_workers=2) as executor:
                batch_results = []
                for link in batch:
                    try:
                        print(f"Fetching components from {link}")
                        time.sleep(
                            random.uniform(2, 5)
                        )  # Random delay between requests
                        result = get_tradingview_sectors_industries_components(link)
                        if result is not None and not result.empty:
                            result = result.assign(component_url=link)
                            batch_results.append(result)
                        else:
                            failed_links.append(link)
                            logging.warning(f"Failed to fetch components from {link}")
                    except Exception as e:
                        failed_links.append(link)
                        logging.error(
                            f"Error fetching components from {link}: {str(e)}"
                        )

                components_arr.extend(batch_results)

            logging.info(
                f"Processed batch {i//BATCH_SIZE + 1}/{(len(components_links) + BATCH_SIZE - 1)//BATCH_SIZE}"
            )

        if not components_arr:
            logging.warning(f"No components found for {country} {dataset}")
            return f"No components found for {country} {dataset}"

        # Combine all results
        components_df = pd.concat(components_arr, ignore_index=True)

        if not components_df.empty:
            # Add timestamp and country info
            components_df["timestamp"] = datetime.now()
            components_df["country"] = country
            components_df["dataset"] = dataset

            # Save to Cassandra
            table_name = "tradingview_icb_components"
            cassandra_client.insert_df_to_cassandra(components_df, table_name)

            # Log summary
            success_count = len(components_df)
            failed_count = len(failed_links)
            logging.info(
                f"Successfully saved {success_count} components for {country} {dataset}"
            )
            if failed_links:
                logging.warning(
                    f"Failed to fetch {failed_count} components for {country} {dataset}"
                )
                logging.warning(f"Failed URLs: {failed_links}")

            return f"Successfully processed {success_count} components for {country} {dataset} (Failed: {failed_count})"
        else:
            return f"No valid components found for {country} {dataset}"

    except Exception as e:
        logging.error(
            f"Error processing components for {country} - {dataset}: {str(e)}"
        )
        raise


def cleanup_cassandra_connection():
    """Cleanup Cassandra connection at the end of DAG run"""
    CassandraClient().close()
    logging.info("Cleaned up Cassandra connection")


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=180),  # Tăng timeout lên 3 giờ
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
}

# Create the DAG
with DAG(
    "tradingview_components_scraper",
    default_args=default_args,
    description="Scrape components for specific country and dataset from TradingView",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    concurrency=3,  # Giới hạn số lượng task chạy đồng thời
    max_active_tasks=3,  # Giới hạn số lượng task active
) as dag:

    start_task = DummyOperator(task_id="start")

    # Get list of countries from JSON file
    all_countries = get_all_countries()

    # Create country task groups
    country_groups = []
    for country in all_countries:
        with TaskGroup(group_id=f"{clean_country_name(country)}") as country_group:
            # Create tasks for each dataset within the country group
            dataset_tasks = []
            for dataset in datasets:
                task_id = f"get_{dataset}_components"
                task = PythonOperator(
                    task_id=task_id,
                    python_callable=get_components_for_country,
                    op_kwargs={
                        "country": country,
                        "dataset": dataset,
                    },
                    retries=3,
                    retry_delay=timedelta(minutes=5),
                    execution_timeout=timedelta(
                        minutes=180
                    ),  # Tăng timeout cho mỗi task
                    pool="default_pool",  # Sử dụng default pool
                    trigger_rule="all_success",  # Chỉ chạy khi upstream task thành công
                )
                dataset_tasks.append(task)

            for i in range(len(dataset_tasks) - 1):
                dataset_tasks[i] >> dataset_tasks[i + 1]

            country_groups.append(country_group)

    cleanup_task = PythonOperator(
        task_id="cleanup_cassandra_connection",
        python_callable=cleanup_cassandra_connection,
        trigger_rule="all_done",
    )

    start_task >> country_groups >> cleanup_task
