from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import os
import logging
import datetime
import json
import pandas as pd
from cassandra.query import SimpleStatement

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CassandraClient:
    def __init__(self):
        self.hosts = os.getenv("CASSANDRA_HOSTS", "cassandra").split(",")
        self.port = int(os.getenv("CASSANDRA_PORT", 9042))
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "my_keyspace")  
        self.session = None
        self.cluster = None

    def connect(self, keyspace=None):
        keyspace_to_create = keyspace if keyspace else self.keyspace
        try:
            logger.info(f"Connecting to Cassandra cluster at hosts: {self.hosts}, port: {self.port}")
            self.cluster = Cluster(
                self.hosts,
                port=self.port,
                protocol_version=5,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
                connect_timeout=15,
                control_connection_timeout=15,
            )
            self.session = self.cluster.connect()
            logger.info("Connected to Cassandra cluster successfully.")
            self.create_keyspace(keyspace_to_create)
            self.session.set_keyspace(keyspace_to_create)
            self.create_tables()
        except Exception as e:
            logger.error(f"Error connecting to Cassandra: {e}", exc_info=True)
            raise

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
            logger.error(f"Failed to create keyspace '{keyspace_to_create}': {e}", exc_info=True)
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
                    timestamp TIMESTAMP,
                    PRIMARY KEY (codesource, component_url)
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
            logger.info(f"Successfully inserted raw data for URL: {data_crawled['url']}")
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
            VALUES ({placeholders}) IF NOT EXISTS
            """
        )
        try:
            for _, row in df.iterrows():
                self.session.execute(query, tuple(row))
            logger.info(f"Inserted DataFrame successfully into {table_name}")
        except Exception as e:
            logger.error(f"Failed to insert DataFrame into {table_name}: {e}", exc_info=True)
            raise

    def query_data(self, query, params=None):
        try:
            if params:
                rows = self.session.execute(query, params)
            else:
                rows = self.session.execute(query)
            return list(rows)
        except Exception as e:
            logger.error(f"Failed to execute query: {e}", exc_info=True)
            raise

    def query_to_dataframe(self, query, params=None):
        try:
            rows = self.query_data(query, params)
            if rows:
                return pd.DataFrame(rows, columns=rows[0]._fields)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to convert query to DataFrame: {e}", exc_info=True)
            raise

    def close(self):
        if self.session and self.cluster:
            self.cluster.shutdown()
            self.session = None
            self.cluster = None
            logger.info("Cassandra connection closed.")