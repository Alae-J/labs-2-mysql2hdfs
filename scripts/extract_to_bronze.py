from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MySQL to HDFS Bronze") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

jdbc_url = "jdbc:mysql://mysql:3306/retail_db"
jdbc_props = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

tables = ["categories", "customers", "departments", "order_items", "orders", "products"]

for table in tables:
    print(f"Extracting {table}...")
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_props)
    df.write.mode("overwrite").csv(f"hdfs://namenode:9000/data/bronze/{table}")
    print(f"  -> {table}: {df.count()} rows written to /data/bronze/{table}")

print("Bronze extraction complete.")
spark.stop()
