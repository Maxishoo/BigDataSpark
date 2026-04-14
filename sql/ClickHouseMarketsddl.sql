-- 1. Витрина продаж по продуктам (mart_products)
CREATE TABLE IF NOT EXISTS mart_products (
    product_id Int64,
    product_name String,
    category_name String,
    total_quantity_sold Int64,
    total_revenue Decimal(10, 2),
    product_rating Decimal(10, 2),
    product_reviews Int64
) ENGINE = MergeTree()
ORDER BY product_id;

-- 2. Витрина по клиентам (mart_clients)
CREATE TABLE IF NOT EXISTS mart_clients (
    customer_id Int64,
    first_name String,
    last_name String,
    email String,
    age Int32,
    country String,
    total_spent Decimal(10, 2),
    avg_check Decimal(10, 2),
    orders_count Int64
) ENGINE = MergeTree()
ORDER BY customer_id;

-- 3. Витрина продаж по времени (mart_date)
CREATE TABLE IF NOT EXISTS mart_date (
    year Int32,
    month Int32,
    monthly_revenue Decimal(10, 2),
    orders_count Int64,
    avg_order_value Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (year, month);

-- 4. Витрина по магазинам (mart_stores)
CREATE TABLE IF NOT EXISTS mart_stores (
    store_id Int64,
    store_name String,
    store_email String,
    store_location String,
    store_city String,
    store_country String,
    total_revenue Decimal(10, 2),
    avg_check Decimal(10, 2),
    orders_count Int64
) ENGINE = MergeTree()
ORDER BY store_id;

-- 5. Витрина по поставщикам (mart_suppliers)
CREATE TABLE IF NOT EXISTS mart_suppliers (
    supplier_id Int64,
    supplier_name String,
    supplier_email String,
    supplier_city String,
    supplier_country String,
    total_revenue Decimal(10, 2),
    avg_price Decimal(10, 2),
    orders_count Int64
) ENGINE = MergeTree()
ORDER BY supplier_id;

-- 6. Витрина качества продукции (mart_quality_products)
CREATE TABLE IF NOT EXISTS mart_quality_products (
    product_id Int64,
    product_name String,
    total_quantity_sold Int64,
    total_revenue Decimal(10, 2),
    product_rating Decimal(10, 2),
    product_reviews Int64
) ENGINE = MergeTree()
ORDER BY product_id;