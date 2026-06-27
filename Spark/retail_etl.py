from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("RetailAnalyticsETL") \
    .getOrCreate()

# Input Paths
sales_path = "gs://retail-landing-bucket/sales/sales.csv"
customer_path = "gs://retail-landing-bucket/customers/customers.csv"
inventory_path = "gs://retail-landing-bucket/inventory/inventory.csv"

# Output Paths
output_path = "gs://retail-curated-bucket/fact_sales"

# Read Sales Data
sales_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(sales_path)

sales_df = sales_df.withColumn("sale_amount", col("sale_amount").cast(DoubleType()))

# Read Customer Data
customer_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(customer_path)

# Read Inventory Data
inventory_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(inventory_path)

print("===== SALES DATA =====")
sales_df.show()
print("===== CUSTOMER DATA =====")
customer_df.show()
print("===== INVENTORY DATA =====")
inventory_df.show()

# Data Validation
sales_df = sales_df.dropna()
customer_df = customer_df.dropna()
inventory_df = inventory_df.dropna()

# Remove Duplicates
sales_df = sales_df.dropDuplicates(["sale_id"])

# Join Operations
joined_df = sales_df \
    .join(customer_df, "customer_id", "left") \
    .join(inventory_df, "product_id", "left")

# Add Processing Timestamp
joined_df = joined_df.withColumn(
    "processing_timestamp",
    current_timestamp()
)

# Window Function Example
window_spec = Window.partitionBy("store_id") \
    .orderBy(desc("sale_amount"))

ranked_df = joined_df.withColumn(
    "sales_rank",
    rank().over(window_spec)
)

# Aggregation Example
agg_df = ranked_df.groupBy("store_id", "category") \
    .agg(
        sum("sale_amount").alias("total_sales"),
        avg("sale_amount").alias("avg_sales"),
        count("sale_id").alias("transaction_count")
    )

print("===== AGGREGATED DATA =====")
agg_df.show()

# Final Fact Table
fact_sales_df = ranked_df.select(
    "sale_id",
    "store_id",
    "product_id",
    "customer_id",
    "quantity",
    "sale_amount",
    "category",
    "membership",
    "sale_date",
    "processing_timestamp"
)

# Write Fact Data
fact_sales_df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("===== FACT SALES WRITTEN SUCCESSFULLY =====")
spark.stop()
