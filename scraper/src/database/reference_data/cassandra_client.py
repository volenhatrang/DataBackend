from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CassandraClient:
    def __init__(self):
        self.host = os.getenv('CASSANDRA_HOST', 'localhost')
        self.port = int(os.getenv('CASSANDRA_PORT', 9042))
        self.keyspace = os.getenv('CASSANDRA_KEYSPACE')
        self.session = None

    def connect(self):
        try:
            cluster = Cluster(
                [self.host],
                port=self.port,
                protocol_version=5,  
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1') 
            )
            self.session = cluster.connect()
            self.create_keyspace()
            self.session.set_keyspace(self.keyspace)
            self.create_tables()
        except Exception as e:
            logger.error(f'Error connecting to Cassandra: {e}')

    def create_keyspace(self, keyspace=None):
        keyspace_to_create = keyspace if keyspace is not None else self.keyspace
        self.session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace_to_create} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        )

    def create_table_data_raw(self):
        self.session.execute(
            f"""
                CREATE TABLE IF NOT EXISTS web_crawl (
                    url text,
                    title text,
                    content text,
                    crawl_date timestamp,
                    data_crawled map<text, text>, 
                    PRIMARY KEY (url, crawl_date)
                ) WITH CLUSTERING ORDER BY (crawl_date DESC);
            """
        )
    
    def create_tables(self):
        self.session.execute("""
                CREATE TABLE IF NOT EXISTS tradingview_exchanges (
                exchange_name TEXT PRIMARY KEY,
                exchange_desc_name TEXT,
                country TEXT,
                types list<TEXT>,
                tab TEXT,
                timestamp TIMESTAMP
            )"""
        )

        self.session.execute("""
                CREATE TABLE IF NOT EXISTS tradingview_countries (
                region TEXT,
                country TEXT,
                data_market TEXT,
                country_flag TEXT,
                PRIMARY KEY (region, country)
            )"""
        )

    def insert_exchanges(self, exchange_data):
        try:
            self.session.execute(
                """
                INSERT INTO tradingview_exchanges (exchange_name, exchange_desc_name, country, types, tab, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    exchange_data['exchange_name'],
                    exchange_data['exchange_desc_name'],
                    exchange_data['country'],
                    exchange_data['types'],
                    exchange_data['tab'],
                    datetime.now()
                )
            )
            logger.info(f"Inserted exchange data for {exchange_data['exchange_name']}")
        except Exception as e:
            logger.error(f"Failed to insert exchange data: {e}")

    def insert_countries(self, country_data):
        try:
            self.session.execute(
                """
                INSERT INTO tradingview_countries (region, country, data_market, country_flag)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    country_data['region'],
                    country_data['country'],
                    country_data['data_market'],
                    country_data['country_flag']
                )
            )
            logger.info(f"Inserted country data for {country_data['country']}")
        except Exception as e:
            logger.error(f"Failed to insert country data: {e}")

    def close(self):
        if self.session:
            self.session.cluster.shutdown()
            logger.info('Closing Cassandra connection')
            self.session = None