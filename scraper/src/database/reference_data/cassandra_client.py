from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
import os
import logging
import datetime
import json
import pandas as pd
from threading import Lock
import time
from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement, BatchType
from cassandra.concurrent import execute_concurrent_with_args
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CassandraClient:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            with self._lock:
                if not self._initialized:
                    self.host = os.getenv("CASSANDRA_HOST", "localhost")
                    self.port = int(os.getenv("CASSANDRA_PORT", 9042))
                    self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "default_keyspace")
                    self.session = None
                    self.cluster = None
                    self._initialized = True
                    # self.connect()

    def connect(self, keyspace=None, retries=3, delay=5):
        with self._lock:
            if self.session is None:
                keyspace_to_create = keyspace if keyspace else self.keyspace
                for attempt in range(retries):
                    try:
                        self.cluster = Cluster(
                            [self.host],
                            port=self.port,
                            protocol_version=5,
                            load_balancing_policy=DCAwareRoundRobinPolicy(
                                local_dc="datacenter1"
                            ),
                        )
                        self.session = self.cluster.connect()
                        self.create_keyspace(keyspace_to_create)
                        self.session.set_keyspace(keyspace_to_create)
                        self.create_tables()
                        logger.info("Connected to Cassandra successfully")
                        return
                    except Exception as e:
                        logger.error(
                            f"Error connecting to Cassandra (attempt {attempt + 1}): {e}"
                        )
                        time.sleep(delay)
                raise Exception(
                    "Failed to connect to Cassandra after multiple attempts"
                )

    def create_keyspace(self, keyspace=None):
        keyspace_to_create = keyspace if keyspace else self.keyspace
        try:
            self.session.execute(
                f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace_to_create}
                WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 2}}
                """
            )
            logger.info(f"Keyspace '{keyspace_to_create}' created or already exists.")
        except Exception as e:
            logger.error(
                f"Failed to create keyspace '{keyspace_to_create}': {e}", exc_info=True
            )
            raise

    def create_tables(self):
        try:
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS web_crawl (
                    url TEXT,
                    title TEXT,
                    content TEXT,
                    crawl_date TIMESTAMP,
                    data_crawled TEXT,
                    PRIMARY KEY (url, crawl_date)
                ) WITH CLUSTERING ORDER BY (crawl_date DESC);
                """
            )
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS tradingview_exchanges (
                    exchange_name TEXT PRIMARY KEY,
                    exchange_desc_name TEXT,
                    country TEXT,
                    types LIST<TEXT>,
                    timestamp TIMESTAMP
                );
                """
            )
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS tradingview_countries (
                    region TEXT,
                    country TEXT,
                    data_market TEXT,
                    country_flag TEXT,
                    timestamp TIMESTAMP,
                    PRIMARY KEY (region, country)
                );
                """
            )
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS tradingview_holidays (
                    exchange TEXT PRIMARY KEY,
                    timezone TEXT,
                    open_time TIME,
                    close_time TIME,
                    regular_duration_hours DOUBLE,
                    holidays LIST<TEXT>,
                    total_trading_days INT,
                    period_start_date DATE,
                    period_end_date DATE,
                    timestamp TIMESTAMP
                );
                """
            )
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS tradingview_sectors (
                    icb_code TEXT,
                    sector TEXT,
                    marketcap DOUBLE,
                    rt DOUBLE,
                    volume DOUBLE,
                    industries TEXT,
                    stocks TEXT,
                    component_url TEXT,
                    country TEXT,
                    cur TEXT,
                    divyield_percent DOUBLE,
                    timestamp TIMESTAMP,
                    PRIMARY KEY (icb_code)
                );
                """
            )
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS tradingview_industries (
                    icb_code TEXT,
                    industry TEXT,
                    marketcap DOUBLE,
                    rt DOUBLE,
                    volume DOUBLE,
                    sector TEXT,
                    stocks TEXT,
                    component_url TEXT,
                    country TEXT,
                    cur TEXT,
                    divyield_percent DOUBLE,
                    timestamp TIMESTAMP,
                    PRIMARY KEY (icb_code)
                );
                """
            )
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS tradingview_icb_components (
                    component_url TEXT,
                    codesource TEXT,
                    marketcap DOUBLE,
                    price DOUBLE,
                    rt DOUBLE,
                    volume DOUBLE,
                    relvolume TEXT,
                    pe TEXT,
                    epsdilttm TEXT,
                    epsdilgrowthttmyoy TEXT,
                    divyieldttm TEXT,
                    analystrating TEXT,
                    stock_url TEXT,
                    ticker TEXT,
                    name TEXT,
                    cur TEXT,
                    country TEXT,
                    dataset TEXT,
                    timestamp TIMESTAMP,
                    PRIMARY KEY (codesource, component_url)
                );
                """
            )
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS all_source_prices (
                    ticker TEXT,
                    date TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    source TEXT,
                    adjclose DOUBLE,
                    updated TIMESTAMP,
                    PRIMARY KEY (ticker, date)
                );
                """
            )
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS download_yah_prices_day_history (
                    ticker TEXT,
                    date TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    source TEXT,
                    adjclose DOUBLE,
                    updated TIMESTAMP,
                    PRIMARY KEY (ticker, date)
                );
                """
            )
            self.session.execute(
                """
                CREATE TABLE IF NOT EXISTS download_yah_prices_intraday_history (
                    ticker TEXT,
                    datetime TIMESTAMP,
                    timestamp TIMESTAMP,
                    date TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    source TEXT,
                    adjclose DOUBLE,
                    updated TIMESTAMP,
                    PRIMARY KEY (ticker, datetime)
                );
                """
            )
            logger.info("All tables created successfully.")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}", exc_info=True)
            raise

    def fetch_latest_data_by_title(self):
        query = "SELECT * FROM web_crawl;"
        try:
            rows = self.session.execute(query)
            latest_data = {}
            for row in rows:
                title = row.title
                crawl_date = row.crawl_date
                if (
                    title not in latest_data
                    or crawl_date > latest_data[title]["crawl_date"]
                ):
                    latest_data[title] = {
                        "url": row.url,
                        "title": row.title,
                        "content": row.content,
                        "crawl_date": row.crawl_date,
                        "data_crawled": row.data_crawled,
                    }
            logger.info("Fetched latest data by title successfully.")
            return latest_data
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}", exc_info=True)
            return {}

    def insert_raw_data(self, data_crawled):
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info(f"Preparing to insert raw data for URL: {data_crawled['url']}")
        try:
            query = SimpleStatement(
                """
                INSERT INTO web_crawl (url, title, content, crawl_date, data_crawled)
                VALUES (%s, %s, %s, %s, %s)
                """
            )
            self.session.execute(
                query,
                (
                    data_crawled["url"],
                    data_crawled["title"],
                    data_crawled["content"],
                    timestamp,
                    data_crawled["data_crawled"],
                ),
            )
            logger.info(
                f"Successfully inserted raw data for URL: {data_crawled['url']}"
            )
        except Exception as e:
            logger.error(f"Failed to insert raw data: {e}", exc_info=True)
            raise

    def insert_exchanges(self, exchange_data):
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        try:
            query = SimpleStatement(
                """
                INSERT INTO tradingview_exchanges (exchange_name, exchange_desc_name, country, types, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                """
            )
            self.session.execute(
                query,
                (
                    exchange_data["exchangeName"],
                    exchange_data["exchangeDescName"],
                    exchange_data["country"],
                    exchange_data["types"],
                    timestamp,
                ),
            )
            logger.info(f"Inserted exchange data for {exchange_data['exchangeName']}")
        except Exception as e:
            logger.error(f"Failed to insert exchange data: {e}", exc_info=True)
            raise

    def insert_countries(self, country_data):
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        try:
            query = SimpleStatement(
                """
                INSERT INTO tradingview_countries (region, country, data_market, country_flag, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                """
            )
            self.session.execute(
                query,
                (
                    country_data["region"],
                    country_data["country"],
                    country_data["data_market"],
                    country_data["country_flag"],
                    timestamp,
                ),
            )
            logger.info(f"Inserted country data for {country_data['country']}")
        except Exception as e:
            logger.error(f"Failed to insert country data: {e}", exc_info=True)
            raise

    def insert_holidays(self, holiday_data):
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        try:
            query = SimpleStatement(
                """
                INSERT INTO tradingview_holidays (exchange, timezone, open_time, close_time, regular_duration_hours, holidays, total_trading_days, period_start_date, period_end_date, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            )
            self.session.execute(
                query,
                (
                    holiday_data["exchange"],
                    holiday_data["timezone"],
                    holiday_data["open_time"],
                    holiday_data["close_time"],
                    holiday_data["regular_duration_hours"],
                    holiday_data["holidays"],
                    holiday_data["total_trading_days"],
                    holiday_data["period_start_date"],
                    holiday_data["period_end_date"],
                    timestamp,
                ),
            )
            logger.info(f"Inserted holiday data for {holiday_data['exchange']}")
        except Exception as e:
            logger.error(f"Failed to insert holiday data: {e}", exc_info=True)
            raise

    def insert_df_to_cassandra(self, df, table_name):
        if df.empty:
            logger.warning(f"DataFrame is empty, skipping insert into {table_name}")
            return

        columns = df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(columns))
        query = SimpleStatement(
            f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
        """
        )

        for _, row in df.iterrows():
            self.session.execute(query, tuple(row))

        logger.info(f"Inserted dataframe successfully to {table_name}!!!")

    def batch_insert_prices_async(
        self,
        df,
        table_name,
        batch_size=1000,
        max_workers=8,
        max_retries=3,
        chunk_size=10000,
    ):
        """
        Optimized for millions of rows with async execution, thread pooling, and chunked processing
        """
        # Prepare the query
        query = f"""
            INSERT INTO {table_name} (ticker, date, open, high, low, close, volume, source, adjclose, updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        prepared_stmt = self.session.prepare(query)

        # Process DataFrame in chunks to manage memory
        total_rows = len(df)
        logging.info(f"🔹 Total rows to process: {total_rows:,}")
        start_time = time.time()

        total_success = 0
        total_failed = 0

        # Process DataFrame in chunks
        for chunk_start in range(0, total_rows, chunk_size):
            # if self._shutdown:
            #     logging.warning("Shutdown requested, stopping processing")
            #     break

            chunk_end = min(chunk_start + chunk_size, total_rows)
            chunk_df = df.iloc[chunk_start:chunk_end]
            params_list = []

            # Convert chunk to parameters
            for _, row in chunk_df.iterrows():
                try:
                    updated_dt = (
                        datetime.datetime.strptime(row["updated"], "%Y-%m-%d %H:%M:%S")
                        if isinstance(row["updated"], str)
                        else row["updated"]
                    )

                    date_val = row["date"]
                    if isinstance(date_val, pd.Timestamp):
                        date_val = date_val.to_pydatetime()
                    elif isinstance(date_val, str):
                        date_val = datetime.datetime.strptime(
                            date_val, "%Y-%m-%d %H:%M:%S"
                        )

                    params = (
                        row["ticker"],
                        date_val,
                        row["open"] if pd.notna(row["open"]) else None,
                        row["high"] if pd.notna(row["high"]) else None,
                        row["low"] if pd.notna(row["low"]) else None,
                        row["close"] if pd.notna(row["close"]) else None,
                        row["volume"] if pd.notna(row["volume"]) else None,
                        row["source"],
                        row["adjclose"] if pd.notna(row["adjclose"]) else None,
                        updated_dt,
                    )
                    params_list.append(params)
                except Exception as e:
                    logging.error(
                        f"⚠️ Error processing row {row['ticker']} on {row['date']}: {e}"
                    )

            # Async execution with retry logic
            def execute_special(params, session, stmt, retries=max_retries):
                for attempt in range(retries):
                    # if self._shutdown:
                    #     return False, (params, "Task shutdown requested")
                    future = session.execute_async(stmt, params)
                    try:
                        future.result()
                        return True, None
                    except Exception as e:
                        if attempt == retries - 1:
                            return False, (params, str(e))
                        time.sleep(1)
                return False, (params, "Max retries reached")

            # Process batch with thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for i in range(0, len(params_list), batch_size):
                    # if self._shutdown:
                    #     logging.warning("Shutdown requested, stopping batch processing")
                    #     break

                    batch = params_list[i : i + batch_size]
                    execute_func = partial(
                        execute_special,
                        session=self.session,
                        stmt=prepared_stmt,
                        retries=max_retries,
                    )

                    futures = {
                        executor.submit(execute_func, params): params
                        for params in batch
                    }

                    batch_success = 0
                    batch_failed = 0

                    for future in as_completed(futures):
                        success, error_info = future.result()
                        if success:
                            batch_success += 1
                        else:
                            batch_failed += 1
                            params, error = error_info
                            logging.error(
                                f"❌ Failed row - ticker: {params[0]}, date: {params[1]}, error: {error}"
                            )

                    total_success += batch_success
                    total_failed += batch_failed

                    elapsed = time.time() - start_time
                    rows_per_sec = total_success / elapsed if elapsed > 0 else 0
                    logging.info(
                        f"🔸 Progress: {total_success:,} success, {total_failed:,} failed "
                        f"({rows_per_sec:,.0f} rows/sec)"
                    )

        # Final report
        elapsed = time.time() - start_time
        logging.info(
            f"✅ Batch insert completed: {total_success:,} success, {total_failed:,} failed "
            f"in {elapsed:.2f} seconds ({total_success/elapsed:,.0f} rows/sec)"
        )
        return total_success, total_failed

    def batch_insert_prices_intraday_async(
        self,
        df,
        table_name,
        batch_size=1000,
        max_workers=8,
        max_retries=3,
        chunk_size=10000,
    ):
        """
        Optimized for millions of rows with async execution, thread pooling, and chunked processing
        """
        # Prepare the query
        query = f"""
            INSERT INTO {table_name} (ticker, date, datetime, timestamp, open, high, low, close, volume, source, adjclose, updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        prepared_stmt = self.session.prepare(query)

        # Process DataFrame in chunks to manage memory
        total_rows = len(df)
        logging.info(f"🔹 Total rows to process: {total_rows:,}")
        start_time = time.time()

        total_success = 0
        total_failed = 0

        # Process DataFrame in chunks
        for chunk_start in range(0, total_rows, chunk_size):
            # if self._shutdown:
            #     logging.warning("Shutdown requested, stopping processing")
            #     break

            chunk_end = min(chunk_start + chunk_size, total_rows)
            chunk_df = df.iloc[chunk_start:chunk_end]
            params_list = []

            # Convert chunk to parameters
            for _, row in chunk_df.iterrows():
                try:
                    updated_dt = (
                        datetime.datetime.strptime(row["updated"], "%Y-%m-%d %H:%M:%S")
                        if isinstance(row["updated"], str)
                        else row["updated"]
                    )

                    datetime_dt = (
                        datetime.datetime.strptime(row["datetime"], "%Y-%m-%d %H:%M:%S")
                        if isinstance(row["datetime"], str)
                        else row["datetime"]
                    )

                    timestamp_dt = (
                        datetime.datetime.strptime(
                            row["timestamp"], "%Y-%m-%d %H:%M:%S"
                        )
                        if isinstance(row["timestamp"], str)
                        else row["timestamp"]
                    )

                    date_val = row["date"]
                    if isinstance(date_val, pd.Timestamp):
                        date_val = date_val.to_pydatetime()
                    elif isinstance(date_val, str):
                        date_val = datetime.datetime.strptime(
                            date_val, "%Y-%m-%d %H:%M:%S"
                        )

                    params = (
                        row["ticker"],
                        date_val,
                        datetime_dt,
                        timestamp_dt,
                        row["open"] if pd.notna(row["open"]) else None,
                        row["high"] if pd.notna(row["high"]) else None,
                        row["low"] if pd.notna(row["low"]) else None,
                        row["close"] if pd.notna(row["close"]) else None,
                        row["volume"] if pd.notna(row["volume"]) else None,
                        row["source"],
                        row["adjclose"] if pd.notna(row["adjclose"]) else None,
                        updated_dt,
                    )
                    params_list.append(params)
                except Exception as e:
                    logging.error(
                        f"⚠️ Error processing row {row['ticker']} on {row['date']}: {e}"
                    )

            # Async execution with retry logic
            def execute_special(params, session, stmt, retries=max_retries):
                for attempt in range(retries):
                    # if self._shutdown:
                    #     return False, (params, "Task shutdown requested")
                    future = session.execute_async(stmt, params)
                    try:
                        future.result()
                        return True, None
                    except Exception as e:
                        if attempt == retries - 1:
                            return False, (params, str(e))
                        time.sleep(1)
                return False, (params, "Max retries reached")

            # Process batch with thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for i in range(0, len(params_list), batch_size):
                    # if self._shutdown:
                    #     logging.warning("Shutdown requested, stopping batch processing")
                    #     break

                    batch = params_list[i : i + batch_size]
                    execute_func = partial(
                        execute_special,
                        session=self.session,
                        stmt=prepared_stmt,
                        retries=max_retries,
                    )

                    futures = {
                        executor.submit(execute_func, params): params
                        for params in batch
                    }

                    batch_success = 0
                    batch_failed = 0

                    for future in as_completed(futures):
                        success, error_info = future.result()
                        if success:
                            batch_success += 1
                        else:
                            batch_failed += 1
                            params, error = error_info
                            logging.error(
                                f"❌ Failed row - ticker: {params[0]}, date: {params[1]}, error: {error}"
                            )

                    total_success += batch_success
                    total_failed += batch_failed

                    elapsed = time.time() - start_time
                    rows_per_sec = total_success / elapsed if elapsed > 0 else 0
                    logging.info(
                        f"🔸 Progress: {total_success:,} success, {total_failed:,} failed "
                        f"({rows_per_sec:,.0f} rows/sec)"
                    )

        # Final report
        elapsed = time.time() - start_time
        logging.info(
            f"✅ Batch insert completed: {total_success:,} success, {total_failed:,} failed "
            f"in {elapsed:.2f} seconds ({total_success/elapsed:,.0f} rows/sec)"
        )
        return total_success, total_failed

    def query_data(self, query, params=None):
        """Execute query with retry logic"""
        return list(self.execute_with_retry(query, params))

    def query_to_dataframe(self, query, params=None):
        """Execute query and return results as DataFrame with retry logic"""
        rows = self.query_data(query, params)
        if rows:
            return pd.DataFrame(rows, columns=rows[0]._fields)
        return pd.DataFrame()

    def close(self):
        with self._lock:
            if self.session:
                self.cluster.shutdown()
                logger.info("Closed Cassandra connection")
                self.session = None
                self.cluster = None

    def ensure_connection(self):
        """Ensure there is a valid connection, reconnect if needed"""
        try:
            if self.session is None or self.cluster is None:
                self.connect()
            else:
                # Test if connection is still alive
                try:
                    self.session.execute("SELECT release_version FROM system.local")
                except Exception:
                    logger.warning("Connection lost, attempting to reconnect...")
                    self.close()  # Close existing connection
                    self.connect()  # Create new connection
            return self.session
        except Exception as e:
            logger.error(f"Error ensuring connection: {e}")
            self.close()  # Force close on error
            raise

    def execute_with_retry(self, query, params=None, max_retries=3):
        """Execute query with retry logic and automatic reconnection"""
        last_error = None
        for attempt in range(max_retries):
            try:
                session = self.ensure_connection()
                result = (
                    session.execute(query, params) if params else session.execute(query)
                )
                return result
            except Exception as e:
                last_error = e
                logger.error(
                    f"Query execution failed (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)  # Exponential backoff
                    self.close()  # Force reconnection on next attempt
                else:
                    logger.error(f"All retry attempts failed. Last error: {last_error}")
                    raise
