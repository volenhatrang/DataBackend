# from pyspark.sql import SparkSession

# # Khởi tạo SparkSession
# spark = (
#     SparkSession.builder.appName("CassandraIntegration")
#     .master("spark://spark-master-1:7077")
#     .config("spark.cassandra.connection.host", "cassandra")
#     .config("spark.cassandra.connection.port", "9042")
#     .getOrCreate()
# )

# # Đọc dữ liệu từ Cassandra
# df = (
#     spark.read.format("org.apache.spark.sql.cassandra")
#     .options(
#         table="download_yah_prices_day_history", keyspace="finance_tradingview_data"
#     )
#     .load()
# )

# # Hiển thị dữ liệu
# df.show()

# # Dừng SparkSession
# spark.stop()
from pyspark.sql import SparkSession

# Step 1: Initialize SparkSession
spark = (
    SparkSession.builder.appName("Create DataFrame Example")
    .master("spark://airflow-docker-spark-master-1:7077")
    .getOrCreate()
)

# Step 2: Define your data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# Step 3: Define your schema
columns = ["Name", "Age"]

# Step 4: Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Step 5: Show the DataFrame
df.show()

# Step 6: Stop the SparkSession
spark.stop()
