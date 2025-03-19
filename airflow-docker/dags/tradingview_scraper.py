from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

from airflow.exceptions import AirflowTaskTimeout

sys.path.extend(
    [
        "/app/scraper/src/fetchers/reference_data/tradingview",
        "/app/scraper/src/database/reference_data",
    ]
)

from sector_and_industries import *
import json
import pandas as pd

# from app.scraper.src.fetchers.reference_data.tradingview.sector_and_industries import *
from datetime import datetime
from airflow.utils.task_group import TaskGroup  # Add this import

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models import Variable
import logging
from cassandra_client import CassandraClient  # pylint: disable=import-error
import timeout_decorator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 17),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


# ==============================
# 🔹 Step 0: load list countries
# ==============================
# Function to get country list
COUNTRIES_JSON_PATH = "/app/scraper/src/database/reference_data/scraping_raw_json/tradingview/countries_with_flags.json"
# COUNTRIES_JSON_PATH = "scraper/src/database/reference_data/scraping_raw_json/tradingview/countries_with_flags.json"


def clean_country_name(country: str) -> str:
    """Convert country name to valid task ID format"""
    # Replace spaces and special characters with underscores
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


def fetch_tradingview_sector_and_industry_by_country(country, dataset):
    cassandra_client = CassandraClient()
    cassandra_client.connect()
    cassandra_client.create_keyspace()
    country_source = country.lower().replace(" ", "-")
    if dataset == "sectors":
        df = get_tradingview_sectors_by_country(country=country_source)
    elif dataset == "industries":
        df = get_tradingview_industries_by_country(country=country_source)

    view_data(df)
    if df is None or df.empty:
        raise ValueError("❌ ERROR: df is None or empty! Check the data source.")

    table_name = f"tradingview_{dataset}"
    cassandra_client.save_to_cassandra(df, table_name)
    cassandra_client.close()

    return f"Fetch successfully {dataset} of {country}!!!!"


# ==============================
# 🔹 Step 2: get components
# ==============================
def fetch_tradingview_sector_and_industry_components_by_country(country, dataset):

    cassandra_client = CassandraClient()
    cassandra_client.connect()
    cassandra_client.create_keyspace()

    table_name = f"tradingview_{dataset}"
    query = f"SELECT * FROM {table_name} WHERE country = %s ALLOW FILTERING"
    components_df = cassandra_client.query_to_dataframe(query, (str(country),))
    view_data(components_df)
    components_links = components_df["component_url"].tolist()
    components_arr = []
    for link in components_links:
        try:
            print(f"Fetching components from {link}")
            result = get_tradingview_sectors_industries_components(link)
            result["component_url"] = link
            view_data(result)
            components_arr.append(result)
        except Exception as e:
            print(f"Failed: {e}")
            components_arr.append(pd.DataFrame())

    components_df = pd.concat(components_arr, ignore_index=True)
    view_data(components_df)

    table_name = f"tradingview_icb_components"
    cassandra_client.save_to_cassandra(components_df, table_name)
    cassandra_client.close()

    return f"fetch successfully {dataset} components of {country}!!!!"


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


# Define DAG
with DAG(
    "trading_view_get_sectors_and_industries",
    default_args=default_args,
    # schedule_interval=timedelta(minutes=90),
    schedule_interval=None,
    catchup=False,
) as dag:

    list_countries_task = PythonOperator(
        task_id="get_list_countries",
        python_callable=get_all_countries,
    )

    with TaskGroup("fetch_sectors_and_industries_tasks") as fetch_data_group:
        for country in all_countries:
            print(country)
            for dataset in datasets:
                PythonOperator(
                    task_id=f"fetch_{clean_country_name(country)}_{dataset}",
                    python_callable=fetch_tradingview_sector_and_industry_by_country,
                    op_args=[country, dataset],
                )

    with TaskGroup("fetch_components_tasks") as fetch_components_group:
        for country in all_countries:
            for dataset in datasets:
                PythonOperator(
                    task_id=f"fetch_{clean_country_name(country)}_{dataset}_components",
                    python_callable=fetch_tradingview_sector_and_industry_components_by_country,
                    op_args=[country, dataset],
                )

    # concat_task = PythonOperator(
    #     task_id="concat_and_save",
    #     python_callable=concat_and_save,
    # )

    # list_countries_task >> fetch_data_group >> fetch_components_group >> concat_task
    list_countries_task >> fetch_data_group >> fetch_components_group


def cleanup():
    logging.info("Closing Cassandra connection.")
    cassandra_client.close()


dag.on_success_callback = cleanup
dag.on_failure_callback = cleanup
