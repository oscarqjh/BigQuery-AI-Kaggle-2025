-- SQL Scripts for Smart E-Commerce Intelligence Engine
-- This file contains all the SQL commands to set up the BigQuery tables

-- 1. Create Products Table
CREATE TABLE IF NOT EXISTS `ecommerce_intelligence.products` (
    product_id STRING NOT NULL,
    name STRING NOT NULL,
    description STRING,
    category STRING,
    brand STRING,
    price FLOAT64,
    rating FLOAT64,
    stock_quantity INT64,
    image_url STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- 2. Create Users Table
CREATE TABLE IF NOT EXISTS `ecommerce_intelligence.users` (
    user_id STRING NOT NULL,
    email STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    demographics STRING,
    user_segment STRING,
    registration_date TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOL
);

-- 3. Create Orders Table
CREATE TABLE IF NOT EXISTS `ecommerce_intelligence.orders` (
    order_id STRING NOT NULL,
    user_id STRING NOT NULL,
    order_date TIMESTAMP NOT NULL,
    total_amount FLOAT64,
    status STRING,
    shipping_address STRING,
    payment_method STRING
);

-- 4. Create Order Items Table
CREATE TABLE IF NOT EXISTS `ecommerce_intelligence.order_items` (
    order_id STRING NOT NULL,
    product_id STRING NOT NULL,
    quantity INT64,
    price FLOAT64,
    total_price FLOAT64
);

-- 5. Create Reviews Table
CREATE TABLE IF NOT EXISTS `ecommerce_intelligence.reviews` (
    review_id STRING NOT NULL,
    product_id STRING NOT NULL,
    user_id STRING NOT NULL,
    rating INT64,
    review_text STRING,
    review_date TIMESTAMP,
    helpful_votes INT64
);

-- 6. Create User Behavior Table
CREATE TABLE IF NOT EXISTS `ecommerce_intelligence.user_behavior` (
    behavior_id STRING NOT NULL,
    user_id STRING NOT NULL,
    product_id STRING,
    action_type STRING,
    timestamp TIMESTAMP,
    session_id STRING,
    quantity INT64
);

-- 7. Create Sales Data Table
CREATE TABLE IF NOT EXISTS `ecommerce_intelligence.sales_data` (
    date DATE NOT NULL,
    product_id STRING NOT NULL,
    quantity_sold INT64,
    revenue FLOAT64,
    orders_count INT64
);

-- 8. Create Product Embeddings Table
CREATE TABLE IF NOT EXISTS `ecommerce_intelligence.product_embeddings` (
    product_id STRING NOT NULL,
    embedding ARRAY<FLOAT64>,
    name STRING,
    description STRING,
    category STRING,
    brand STRING,
    created_at TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_products_category ON `ecommerce_intelligence.products`(category);
CREATE INDEX IF NOT EXISTS idx_products_brand ON `ecommerce_intelligence.products`(brand);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON `ecommerce_intelligence.orders`(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON `ecommerce_intelligence.orders`(order_date);
CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON `ecommerce_intelligence.reviews`(product_id);
CREATE INDEX IF NOT EXISTS idx_user_behavior_user_id ON `ecommerce_intelligence.user_behavior`(user_id);
CREATE INDEX IF NOT EXISTS idx_user_behavior_product_id ON `ecommerce_intelligence.user_behavior`(product_id);

-- Create vector index for embeddings
CREATE VECTOR INDEX IF NOT EXISTS product_embeddings_index
ON `ecommerce_intelligence.product_embeddings`(embedding)
OPTIONS (distance_type = 'COSINE');
