from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, col, sum, count, avg, first

spark = SparkSession.builder \
    .appName("StarSchemaETL") \
    .getOrCreate()

POSTGRES_URL = "jdbc:postgresql://postgres:5432/lab_db"
POSTGRES_PROPS = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}


dim_customer = spark.read.jdbc(POSTGRES_URL, "dim_customer", properties=POSTGRES_PROPS)
dim_product = spark.read.jdbc(POSTGRES_URL, "dim_product", properties=POSTGRES_PROPS)
dim_seller = spark.read.jdbc(POSTGRES_URL, "dim_seller", properties=POSTGRES_PROPS)
dim_store = spark.read.jdbc(POSTGRES_URL, "dim_store", properties=POSTGRES_PROPS)
dim_supplier = spark.read.jdbc(POSTGRES_URL, "dim_supplier", properties=POSTGRES_PROPS)
dim_product_category = spark.read.jdbc(POSTGRES_URL, "dim_product_category", properties=POSTGRES_PROPS)
fact_sales = spark.read.jdbc(POSTGRES_URL, "fact_sales", properties=POSTGRES_PROPS)

# 1 Витрина продаж по продуктам
sales_with_products = fact_sales.join(
    dim_product,
    fact_sales["product_id"] == dim_product["product_id"],
    how="left"
).join(
    dim_product_category,
    dim_product["category_id"] == dim_product_category["category_id"],
    how="left"
).groupBy(
    fact_sales["product_id"]
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum(col("quantity") * col("price")).alias("total_revenue"),
    first("product_name").alias("product_name"),
    first("name").alias("category_name"),
    first("product_rating").alias("product_rating"),
    first("product_reviews").alias("product_reviews")
)

print(sales_with_products.head())

# 2 Витрина по клиентам
sales_with_clients = fact_sales.join(
    dim_customer,
    fact_sales["customer_id"] == dim_customer["customer_id"],
    how="left"
).groupBy(
    fact_sales["customer_id"]
).agg(
    sum(col("quantity") * col("price")).alias("total_spent"),
    avg(col("quantity") * col("price")).alias("avg_check"),
    count("sale_id").alias("orders_count"),
    first("first_name").alias("first_name"),
    first("last_name").alias("last_name"),
    first("email").alias("email"),
    first("age").alias("age"),
    first("country").alias("country")
)

# 3 витрина продаж по времени
sales_with_date = fact_sales.withColumn(
    "sale_date", to_date(col("date"), "M/d/yyyy")
).groupBy(
    year("sale_date").alias("year"),
    month("sale_date").alias("month")
).agg(
    sum(col("quantity") * col("price")).alias("monthly_revenue"),
    count("sale_id").alias("orders_count"),
    avg(col("quantity") * col("price")).alias("avg_order_value")
)

# 4 витрина по магазинам
sales_with_stores = fact_sales.join(
    dim_store,
    fact_sales["store_id"] == dim_store["store_id"],
    how="left"
).groupBy(
    fact_sales["store_id"]
).agg(
    sum(col("quantity") * col("price")).alias("total_revenue"),
    avg(col("quantity") * col("price")).alias("avg_check"),
    count("sale_id").alias("orders_count"),
    first("store_name").alias("store_name"),
    first("store_email").alias("store_email"),
    first("store_location").alias("store_location"),
    first("store_city").alias("store_city"),
    first("store_country").alias("store_country")
)

# 5 витрина по поставщикам
sales_with_suppliers = fact_sales.join(
    dim_supplier,
    fact_sales["supplier_id"] == dim_supplier["supplier_id"],
    how="left"
).groupBy(
    fact_sales["supplier_id"]
).agg(
    sum(col("quantity") * col("price")).alias("total_revenue"),
    avg(col("price")).alias("avg_price"),
    count("sale_id").alias("orders_count"),
    first("supplier_name").alias("supplier_name"),
    first("supplier_email").alias("supplier_email"),
    first("supplier_city").alias("supplier_city"),
    first("supplier_country").alias("supplier_country")
)

# 6 витрина по продукции
sales_quality_products = fact_sales.join(
    dim_product,
    fact_sales["product_id"] == dim_product["product_id"],
    how="left"
).groupBy(
    fact_sales["product_id"]
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum(col("quantity") * col("price")).alias("total_revenue"),
    first("product_name").alias("product_name"),
    first("product_rating").alias("product_rating"),
    first("product_reviews").alias("product_reviews")
)

# загружаем витрины в clickhouse

clickhouse_url = "jdbc:clickhouse://clickhouse:8123/lab_db"

clickhouse_properties = {
    "user": "default",
    "password": "",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}
#1
sales_with_products.write \
    .mode("append") \
    .jdbc(url=clickhouse_url, table="mart_products", properties=clickhouse_properties)
print("✅ ClickHouse: Таблица 1 загружена")
#2
sales_with_clients.write \
    .mode("append") \
    .jdbc(url=clickhouse_url, table="mart_clients", properties=clickhouse_properties)
print("✅ ClickHouse: Таблица 2 загружена")
#3
sales_with_date.write \
    .mode("append") \
    .jdbc(url=clickhouse_url, table="mart_date", properties=clickhouse_properties)
print("✅ ClickHouse: Таблица 3 загружена")
#4
sales_with_stores.write \
    .mode("append") \
    .jdbc(url=clickhouse_url, table="mart_stores", properties=clickhouse_properties)
print("✅ ClickHouse: Таблица 4 загружена")
#5
sales_with_suppliers.write \
    .mode("append") \
    .jdbc(url=clickhouse_url, table="mart_suppliers", properties=clickhouse_properties)
print("✅ClickHouse: Таблица 5 загружена")
#6
sales_quality_products.write \
    .mode("append") \
    .jdbc(url=clickhouse_url, table="mart_quality_products", properties=clickhouse_properties)
print("✅ ClickHouse: Таблица 6 загружена")

print("\n✅ Все витрины загружены")