from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, year, month, dayofmonth, round

spark = SparkSession.builder \
    .appName("Silver to Gold") \
    .getOrCreate()

hdfs = "hdfs://namenode:9000"

# ---- Read Silver layer ----
sales = spark.read.parquet(f"{hdfs}/data/silver/sales")
customers = spark.read.parquet(f"{hdfs}/data/silver/customers")
products = spark.read.parquet(f"{hdfs}/data/silver/products")
categories = spark.read.parquet(f"{hdfs}/data/silver/categories")
departments = spark.read.parquet(f"{hdfs}/data/silver/departments")

# ---- Q16: Fact table - fact_sales ----
fact_sales = sales.select(
    col("order_item_id"),
    col("order_id"),
    col("order_date"),
    col("customer_id"),
    col("product_id"),
    col("category_id"),
    col("department_id"),
    col("order_item_quantity").alias("quantity"),
    col("order_item_product_price").alias("unit_price"),
    col("order_item_subtotal").alias("revenue"),
    col("order_status"),
    col("customer_state").alias("region")
)

print(f"fact_sales: {fact_sales.count()} rows")
fact_sales.write.mode("overwrite").parquet(f"{hdfs}/data/gold/fact_sales")

# ---- Q17: Dimension tables ----

# dim_time
dim_time = sales.select(
    col("order_date"),
    year(col("order_date")).alias("year"),
    month(col("order_date")).alias("month"),
    dayofmonth(col("order_date")).alias("day")
).dropDuplicates()

dim_time.write.mode("overwrite").parquet(f"{hdfs}/data/gold/dim_time")
print(f"dim_time: {dim_time.count()} rows")

# dim_customer
dim_customer = customers.select(
    col("customer_id"),
    col("customer_fname"),
    col("customer_lname"),
    col("customer_city"),
    col("customer_state"),
    col("customer_zipcode")
).dropDuplicates()

dim_customer.write.mode("overwrite").parquet(f"{hdfs}/data/gold/dim_customer")
print(f"dim_customer: {dim_customer.count()} rows")

# dim_product
dim_product = products.join(categories, products.product_category_id == categories.category_id) \
    .join(departments, categories.category_department_id == departments.department_id) \
    .select(
        col("product_id"),
        col("product_name"),
        col("product_price"),
        col("category_id"),
        col("category_name"),
        col("department_id"),
        col("department_name")
    ).dropDuplicates()

dim_product.write.mode("overwrite").parquet(f"{hdfs}/data/gold/dim_product")
print(f"dim_product: {dim_product.count()} rows")

# ---- Q18: Aggregated reporting tables ----

# Revenue by month
revenue_by_month = fact_sales.groupBy(
    year(col("order_date")).alias("year"),
    month(col("order_date")).alias("month")
).agg(
    round(sum("revenue"), 2).alias("total_revenue"),
    sum("quantity").alias("total_quantity"),
    count("order_item_id").alias("total_transactions")
).orderBy("year", "month")

revenue_by_month.write.mode("overwrite").parquet(f"{hdfs}/data/gold/agg_revenue_by_month")
print("agg_revenue_by_month:")
revenue_by_month.show()

# Revenue by category
revenue_by_category = fact_sales \
    .join(dim_product, "product_id") \
    .groupBy("category_name", "department_name") \
    .agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        sum("quantity").alias("total_quantity")
    ).orderBy(col("total_revenue").desc())

revenue_by_category.write.mode("overwrite").parquet(f"{hdfs}/data/gold/agg_revenue_by_category")
print("agg_revenue_by_category:")
revenue_by_category.show()

# Revenue by region
revenue_by_region = fact_sales.groupBy("region").agg(
    round(sum("revenue"), 2).alias("total_revenue"),
    count("order_item_id").alias("total_transactions")
).orderBy(col("total_revenue").desc())

revenue_by_region.write.mode("overwrite").parquet(f"{hdfs}/data/gold/agg_revenue_by_region")
print("agg_revenue_by_region:")
revenue_by_region.show()

print("Gold layer complete.")
spark.stop()
