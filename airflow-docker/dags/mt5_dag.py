# import os
# from dotenv import load_dotenv
# from airflow import DAG

# # from airflow.operators.http import SimpleHttpOperator
# from airflow.operators.python import PythonOperator
# from airflow.models import Variable
# from datetime import datetime, timedelta
# import requests
# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# load_dotenv()

# MT5_API_DOMAIN = os.getenv("MT5_API_DOMAIN") or Variable.get(
#     "MT5_API_DOMAIN", default_var="http://127.0.0.1:8000"
# )

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 3,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     "mt5_data_pipeline",
#     default_args=default_args,
#     description="DAG to pull and process MT5 data",
#     schedule_interval=None,
#     start_date=datetime(2025, 3, 18),
#     catchup=False,
# ) as dag:

#     pull_mt5_task = SimpleHttpOperator(
#         task_id="pull_mt5_data",
#         method="GET",
#         endpoint="/cron/pull-mt5",
#         http_conn_id="mt5_api",
#         log_response=True,
#         response_check=lambda response: response.json().get("status") == True,
#     )

#     get_equity_task = SimpleHttpOperator(
#         task_id="get_current_equity",
#         method="GET",
#         endpoint="/dapp/equity",
#         http_conn_id="mt5_api",
#         log_response=True,
#     )

#     def process_timeseries_data():
#         try:
#             url = f"{MT5_API_DOMAIN}/dapp/timeseries/equity?start_timestamp=1710705600&end_timestamp=1710792000&timeframe=H1"
#             response = requests.get(url, timeout=10)
#             response.raise_for_status()
#             data = response.json()
#             logger.info(f"Processed timeseries data: {data}")
#             return data
#         except requests.exceptions.RequestException as e:
#             logger.error(f"Error fetching timeseries data: {str(e)}")
#             raise

#     process_timeseries_task = PythonOperator(
#         task_id="process_timeseries_data",
#         python_callable=process_timeseries_data,
#     )

#     def save_data_to_file(ti):
#         data = ti.xcom_pull(task_ids="process_timeseries_data")
#         if data:
#             with open("/tmp/mt5_data.txt", "a") as f:
#                 f.write(f"{datetime.now()}: {data}\n")
#             logger.info("Data saved to file")

#     save_data_task = PythonOperator(
#         task_id="save_data",
#         python_callable=save_data_to_file,
#     )

#     pull_mt5_task >> get_equity_task >> process_timeseries_task >> save_data_task
