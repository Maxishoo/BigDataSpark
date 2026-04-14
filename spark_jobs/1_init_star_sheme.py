from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_date, trim
from pathlib import Path

spark = SparkSession.builder \
    .appName("StarSchemaETL") \
    .getOrCreate()

BASE_DIR = Path(__file__).resolve().parent.parent

POSTGRES_URL = "jdbc:postgresql://postgres:5432/lab_db"
POSTGRES_PROPS = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# 1. RAW
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("customer_first_name", StringType(), False),
    StructField("customer_last_name", StringType(), False),
    StructField("customer_age", IntegerType(), False),
    StructField("customer_email", StringType(), False),
    StructField("customer_country", StringType(), False),
    StructField("customer_postal_code", StringType(), False),
    StructField("customer_pet_type", StringType(), False),
    StructField("customer_pet_name", StringType(), False),
    StructField("customer_pet_breed", StringType(), False),
    StructField("seller_first_name", StringType(), False),
    StructField("seller_last_name", StringType(), False),
    StructField("seller_email", StringType(), False),
    StructField("seller_country", StringType(), False),
    StructField("seller_postal_code", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("product_category", StringType(), False),
    StructField("product_price", DecimalType(10, 2), False),
    StructField("product_quantity", IntegerType(), False),
    StructField("sale_date", StringType(), False),
    StructField("sale_customer_id", IntegerType(), False),
    StructField("sale_seller_id", IntegerType(), False),
    StructField("sale_product_id", IntegerType(), False),
    StructField("sale_quantity", IntegerType(), False),
    StructField("sale_total_price", DecimalType(12, 2), False),
    StructField("store_name", StringType(), False),
    StructField("store_location", StringType(), False),
    StructField("store_city", StringType(), False),
    StructField("store_state", StringType(), False),
    StructField("store_country", StringType(), False),
    StructField("store_phone", StringType(), False),
    StructField("store_email", StringType(), False),
    StructField("pet_category", StringType(), False),
    StructField("product_weight", DecimalType(10, 2), False),
    StructField("product_color", StringType(), False),
    StructField("product_size", StringType(), False),
    StructField("product_brand", StringType(), False),
    StructField("product_material", StringType(), False),
    StructField("product_description", StringType(), False),
    StructField("product_rating", DecimalType(3, 2), False),
    StructField("product_reviews", IntegerType(), False),
    StructField("product_release_date", StringType(), False),
    StructField("product_expiry_date", StringType(), False),
    StructField("supplier_name", StringType(), False),
    StructField("supplier_contact", StringType(), False),
    StructField("supplier_email", StringType(), False),
    StructField("supplier_phone", StringType(), False),
    StructField("supplier_address", StringType(), False),
    StructField("supplier_city", StringType(), False),
    StructField("supplier_country", StringType(), False)
])

df_raw = spark.read \
    .option("header", "true") \
    .option("sep", ",") \
    .option("multiLine", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("mode", "FAILFAST") \
    .schema(schema) \
    .csv(f"{BASE_DIR}/data/*.csv") \
    .na.fill("")

df_raw = df_raw \
    .withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy")) \
    .withColumn("product_release_date", to_date(col("product_release_date"), "M/d/yyyy")) \
    .withColumn("product_expiry_date", to_date(col("product_expiry_date"), "M/d/yyyy"))

# 2. признаки dim
#  PET 
dim_pet = df_raw.select(
    col("customer_pet_type").alias("pet_type"),
    col("customer_pet_name").alias("pet_name"),
    col("customer_pet_breed").alias("pet_breed")
).distinct()

dim_pet.write.mode("append").jdbc(POSTGRES_URL, "dim_pet", properties=POSTGRES_PROPS)
dim_pet_db = spark.read.jdbc(POSTGRES_URL, "dim_pet", properties=POSTGRES_PROPS)

#  CUSTOMER 
dim_customer = df_raw \
    .join(dim_pet_db,
          on=[
              df_raw["customer_pet_type"] == dim_pet_db["pet_type"],
              df_raw["customer_pet_name"] == dim_pet_db["pet_name"],
              df_raw["customer_pet_breed"] == dim_pet_db["pet_breed"]
          ],
          how="left") \
    .select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code"),
        col("pet_id")
    ).distinct()

dim_customer.write.mode("append").jdbc(POSTGRES_URL, "dim_customer", properties=POSTGRES_PROPS)
dim_customer_db = spark.read.jdbc(POSTGRES_URL, "dim_customer", properties=POSTGRES_PROPS)

#  SELLER 
dim_seller = df_raw.select(
    col("seller_first_name").alias("first_name"),
    col("seller_last_name").alias("last_name"),
    col("seller_email").alias("email"),
    col("seller_country").alias("country"),
    col("seller_postal_code").alias("postal_code")
).distinct()

dim_seller.write.mode("append").jdbc(POSTGRES_URL, "dim_seller", properties=POSTGRES_PROPS)
dim_seller_db = spark.read.jdbc(POSTGRES_URL, "dim_seller", properties=POSTGRES_PROPS)

#  CATEGORY 
dim_category = df_raw.select(
    col("product_category").alias("name"),
    col("pet_category")
).distinct()

dim_category.write.mode("append").jdbc(POSTGRES_URL, "dim_product_category", properties=POSTGRES_PROPS)
dim_category_db = spark.read.jdbc(POSTGRES_URL, "dim_product_category", properties=POSTGRES_PROPS)

#  PRODUCT 
dim_product = df_raw \
    .join(dim_category_db,
          on=[
              df_raw["product_category"] == dim_category_db["name"],
              df_raw["pet_category"] == dim_category_db["pet_category"]
          ],
          how="left") \
    .select(
        col("product_name"),
        col("category_id"),
        col("product_weight"),
        col("product_color"),
        col("product_size"),
        col("product_brand"),
        col("product_material"),
        col("product_description"),
        col("product_rating"),
        col("product_reviews"),
        col("product_release_date"),
        col("product_expiry_date")
    ).distinct()

dim_product.write.mode("append").jdbc(POSTGRES_URL, "dim_product", properties=POSTGRES_PROPS)
dim_product_db = spark.read.jdbc(POSTGRES_URL, "dim_product", properties=POSTGRES_PROPS)

#  STORE 
dim_store = df_raw.select(
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email"
).distinct()

dim_store.write.mode("append").jdbc(POSTGRES_URL, "dim_store", properties=POSTGRES_PROPS)
dim_store_db = spark.read.jdbc(POSTGRES_URL, "dim_store", properties=POSTGRES_PROPS)

#  SUPPLIER 
dim_supplier = df_raw.select(
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country"
).distinct()

dim_supplier.write.mode("append").jdbc(POSTGRES_URL, "dim_supplier", properties=POSTGRES_PROPS)
dim_supplier_db = spark.read.jdbc(POSTGRES_URL, "dim_supplier", properties=POSTGRES_PROPS)

# 3. факт: продажи
fact_sales = df_raw \
    .join(dim_customer_db,
          on=[
              df_raw["customer_first_name"] == dim_customer_db["first_name"],
              df_raw["customer_last_name"] == dim_customer_db["last_name"],
              df_raw["customer_age"] == dim_customer_db["age"],
              df_raw["customer_email"] == dim_customer_db["email"],
              df_raw["customer_country"] == dim_customer_db["country"],
              df_raw["customer_postal_code"] == dim_customer_db["postal_code"]
          ], how="left") \
    .join(dim_seller_db,
          on=[
              df_raw["seller_first_name"] == dim_seller_db["first_name"],
              df_raw["seller_last_name"] == dim_seller_db["last_name"],
              df_raw["seller_email"] == dim_seller_db["email"],
              df_raw["seller_country"] == dim_seller_db["country"],
              df_raw["seller_postal_code"] == dim_seller_db["postal_code"]
          ], how="left") \
    .join(dim_category_db,
          on=[
              df_raw["product_category"] == dim_category_db["name"],
              df_raw["pet_category"] == dim_category_db["pet_category"]
          ], how="left") \
    .join(dim_product_db,
          on=[
              df_raw["product_name"] == dim_product_db["product_name"],
              dim_category_db["category_id"] == dim_product_db["category_id"],
              df_raw["product_weight"] == dim_product_db["product_weight"],
              df_raw["product_color"] == dim_product_db["product_color"],
              df_raw["product_size"] == dim_product_db["product_size"],
              df_raw["product_brand"] == dim_product_db["product_brand"],
              df_raw["product_material"] == dim_product_db["product_material"],
              df_raw["product_description"] == dim_product_db["product_description"],
              df_raw["product_rating"] == dim_product_db["product_rating"],
              df_raw["product_reviews"] == dim_product_db["product_reviews"],
              df_raw["product_release_date"] == dim_product_db["product_release_date"],
              df_raw["product_expiry_date"] == dim_product_db["product_expiry_date"]
          ], how="left") \
    .join(dim_store_db,
          on=[
              df_raw["store_name"] == dim_store_db["store_name"],
              df_raw["store_location"] == dim_store_db["store_location"],
              df_raw["store_city"] == dim_store_db["store_city"],
              df_raw["store_state"] == dim_store_db["store_state"],
              df_raw["store_country"] == dim_store_db["store_country"],
              df_raw["store_phone"] == dim_store_db["store_phone"],
              df_raw["store_email"] == dim_store_db["store_email"]
          ], how="left") \
    .join(dim_supplier_db,
          on=[
              df_raw["supplier_name"] == dim_supplier_db["supplier_name"],
              df_raw["supplier_contact"] == dim_supplier_db["supplier_contact"],
              df_raw["supplier_email"] == dim_supplier_db["supplier_email"],
              df_raw["supplier_phone"] == dim_supplier_db["supplier_phone"],
              df_raw["supplier_address"] == dim_supplier_db["supplier_address"],
              df_raw["supplier_city"] == dim_supplier_db["supplier_city"],
              df_raw["supplier_country"] == dim_supplier_db["supplier_country"]
          ], how="left") \
    .select(
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("store_id"),
        col("supplier_id"),
        col("sale_date").alias("date"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("price")
    )
fact_sales.write.mode("append").jdbc(POSTGRES_URL, "fact_sales", properties=POSTGRES_PROPS)

# проверим, что у нас нужное количество строк везде fact 10k
with open("tt.txt", "w") as file:
    file.write(f"RAW {df_raw.count()}\n")
    file.write(f"fact {fact_sales.count()}\n")
    file.write(f"dim_category_db {dim_category_db.count()}\n")
    file.write(f"dim_customer_db {dim_customer_db.count()}\n")
    file.write(f"dim_pet_db {dim_pet_db.count()}\n")
    file.write(f"dim_product_db {dim_product_db.count()}\n")
    file.write(f"dim_seller_db {dim_seller_db.count()}\n")
    file.write(f"dim_store_db {dim_store_db.count()}\n")
    file.write(f"dim_supplier_db {dim_supplier_db.count()}\n")

print("All done!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
