from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import os
import logging
import datetime
import json
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CassandraClient:
    def __init__(self):
        self.host = os.getenv("CASSANDRA_HOST", "localhost")
        self.port = int(os.getenv("CASSANDRA_PORT", 9042))
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "default_keyspace")
        self.session = None

    def connect(self, keyspace=None):
        keyspace_to_create = keyspace if keyspace else self.keyspace
        try:
            cluster = Cluster(
                [self.host],
                port=self.port,
                protocol_version=5,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
            )
            self.session = cluster.connect()
            self.create_keyspace(keyspace_to_create)
            self.session.set_keyspace(keyspace_to_create)
            self.create_tables()
        except Exception as e:
            logger.error(f"Error connecting to Cassandra: {e}")

    def create_keyspace(self, keyspace=None):
        keyspace_to_create = keyspace if keyspace else self.keyspace
        try:
            self.session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {keyspace_to_create} "
                f"WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
            )
        except Exception as e:
            logger.error(f"Failed to create keyspace: {e}")

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
                    timestamp TIMESTAMP,
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
                    PRIMARY KEY (codesource)
                );
            """
            )
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")

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
            logger.error(f"Failed to fetch data: {e}")
            return {}

    def insert_raw_data(self, data_crawled):
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        logger.info(f"Preparing to insert raw data for URL: {data_crawled['url']}")
        try:
            logger.debug(f"Cassandra session: {self.session}")
            logger.debug(f"Data to insert: {data_crawled}")
            query = """
                INSERT INTO web_crawl (url, title, content, crawl_date, data_crawled)
                VALUES (%s, %s, %s, %s, %s)
            """
            logger.info("Executing Cassandra insert...")
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
            logger.error(f"Failed to insert raw data: {str(e)}", exc_info=True)
            raise

    def insert_exchanges(self, exchange_data):
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        try:
            self.session.execute(
                """
                INSERT INTO tradingview_exchanges (exchange_name, exchange_desc_name, country, types, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                """,
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
            logger.error(f"Failed to insert exchange data: {e}")

    def insert_countries(self, country_data):
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        try:
            self.session.execute(
                """
                INSERT INTO tradingview_countries (region, country, data_market, country_flag, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                """,
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
            logger.error(f"Failed to insert country data: {e}")

    def insert_holidays(self, holiday_data):
        timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
        try:
            self.session.execute(
                """
                INSERT INTO tradingview_holidays (exchange, timezone, open_time, close_time, regular_duration_hours, holidays, total_trading_days, period_start_date, period_end_date, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
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
            logger.info(f"Inserted country data for holidays")
        except Exception as e:
            logger.error(f"Failed to insert country data: {e}")

    def save_to_cassandra(self, df, table_name):
        if df.empty:
            return

        columns = df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) 
            VALUES ({placeholders}) IF NOT EXISTS
        """
        for _, row in df.iterrows():
            self.session.execute(query, tuple(row))

        logger.info(f"Inserted dataframe sucessfully to {table_name}!!!")

    def query_data(self, query, params=None):
        if params:
            rows = self.session.execute(query, params)
        else:
            rows = self.session.execute(query)
        return list(rows)

    def query_to_dataframe(self, query, params=None):
        rows = self.query_data(query, params)
        if rows:
            return pd.DataFrame(rows, columns=rows[0]._fields)
        return pd.DataFrame()

    def close(self):
        if self.session:
            self.session.cluster.shutdown()
            logger.info("Closed Cassandra connection")
            self.session = None
