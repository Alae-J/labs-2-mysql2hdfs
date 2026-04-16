from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month

spark = SparkSession.builder \
    .appName("Bronze to Silver") \
    .getOrCreate()

hdfs = "hdfs://namenode:9000"

# ---- Read Bronze layer (CSV) ----
categories = spark.read.csv(f"{hdfs}/data/bronze/categories", inferSchema=True, header=False) \
    .toDF("category_id", "category_department_id", "category_name")

customers = spark.read.csv(f"{hdfs}/data/bronze/customers", inferSchema=True, header=False) \
    .toDF("customer_id", "customer_fname", "customer_lname", "customer_email",
          "customer_password", "customer_street", "customer_city", "customer_state", "customer_zipcode")

departments = spark.read.csv(f"{hdfs}/data/bronze/departments", inferSchema=True, header=False) \
    .toDF("department_id", "department_name")

order_items = spark.read.csv(f"{hdfs}/data/bronze/order_items", inferSchema=True, header=False) \
    .toDF("order_item_id", "order_item_order_id", "order_item_product_id",
          "order_item_quantity", "order_item_subtotal", "order_item_product_price")

orders = spark.read.csv(f"{hdfs}/data/bronze/orders", inferSchema=True, header=False) \
    .toDF("order_id", "order_date", "order_customer_id", "order_status")

products = spark.read.csv(f"{hdfs}/data/bronze/products", inferSchema=True, header=False) \
    .toDF("product_id", "product_category_id", "product_name",
          "product_description", "product_price", "product_image")

# ---- Q9: Remove duplicates ----
categories = categories.dropDuplicates()
customers = customers.dropDuplicates()
departments = departments.dropDuplicates()
order_items = order_items.dropDuplicates()
orders = orders.dropDuplicates()
products = products.dropDuplicates()

print(f"After dedup - orders: {orders.count()}, order_items: {order_items.count()}")

# ---- Q10: Handle missing or null values ----
customers = customers.fillna({"customer_email": "unknown", "customer_street": "unknown"})
products = products.fillna({"product_description": "N/A"})
order_items = order_items.dropna(subset=["order_item_order_id", "order_item_product_id"])

# ---- Q11: Standardize date formats ----
orders = orders.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd HH:mm:ss.S"))

# ---- Q12: Filter invalid transactions ----
order_items = order_items.filter(col("order_item_quantity") > 0)
order_items = order_items.filter(col("order_item_subtotal") >= 0)

print(f"After filtering - order_items: {order_items.count()}")

# ---- Q13: Join tables to create clean dataset ----
silver_dataset = order_items \
    .join(orders, order_items.order_item_order_id == orders.order_id, "inner") \
    .join(products, order_items.order_item_product_id == products.product_id, "inner") \
    .join(categories, products.product_category_id == categories.category_id, "inner") \
    .join(departments, categories.category_department_id == departments.department_id, "inner") \
    .join(customers, orders.order_customer_id == customers.customer_id, "inner")

# Add year and month columns for partitioning
silver_dataset = silver_dataset \
    .withColumn("order_year", year(col("order_date"))) \
    .withColumn("order_month", month(col("order_date")))

print(f"Silver dataset: {silver_dataset.count()} rows")
silver_dataset.printSchema()

# ---- Q14 & Q15: Write as Parquet, partitioned by year/month ----
silver_dataset.write.mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .parquet(f"{hdfs}/data/silver/sales")

# Also save individual clean tables for flexibility
customers.write.mode("overwrite").parquet(f"{hdfs}/data/silver/customers")
products.write.mode("overwrite").parquet(f"{hdfs}/data/silver/products")
categories.write.mode("overwrite").parquet(f"{hdfs}/data/silver/categories")
departments.write.mode("overwrite").parquet(f"{hdfs}/data/silver/departments")
orders.write.mode("overwrite").parquet(f"{hdfs}/data/silver/orders")

print("Silver layer complete.")
spark.stop()
