-- AI-Powered SQL Queries for Smart E-Commerce Intelligence Engine
-- This file contains examples of using BigQuery AI functions

-- 1. Generate Personalized Marketing Content
-- Create personalized marketing emails based on user behavior
SELECT 
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    AI.GENERATE(
        'text-bison@001',
        CONCAT(
            'Create a personalized marketing email for ', u.first_name, ' ',
            'who recently purchased products in the ', 
            COALESCE(p.category, 'electronics'), ' category. ',
            'Their average order value is $', CAST(AVG(o.total_amount) AS STRING), '. ',
            'Make it engaging and include a special discount offer.'
        ),
        500,  -- max_tokens
        0.7,  -- temperature
        0.8,  -- top_p
        40    -- top_k
    ) AS personalized_email
FROM `ecommerce_intelligence.users` u
LEFT JOIN `ecommerce_intelligence.orders` o ON u.user_id = o.user_id
LEFT JOIN `ecommerce_intelligence.order_items` oi ON o.order_id = oi.order_id
LEFT JOIN `ecommerce_intelligence.products` p ON oi.product_id = p.product_id
WHERE u.is_active = true
GROUP BY u.user_id, u.first_name, u.last_name, u.email, p.category
LIMIT 10;

-- 2. Generate Product Embeddings
-- Create embeddings for product descriptions
SELECT 
    product_id,
    name,
    description,
    ML.GENERATE_EMBEDDING(
        'textembedding-gecko@001',
        CONCAT(name, ' ', COALESCE(description, ''), ' ', category, ' ', brand)
    ) AS embedding
FROM `ecommerce_intelligence.products`
WHERE description IS NOT NULL;

-- 3. Semantic Product Search
-- Find similar products using vector search
SELECT 
    p1.product_id,
    p1.name,
    p1.description,
    p1.price,
    p1.category,
    distance
FROM VECTOR_SEARCH(
    TABLE `ecommerce_intelligence.product_embeddings`,
    'embedding',
    (SELECT embedding FROM `ecommerce_intelligence.product_embeddings` WHERE product_id = 'PROD001'),
    top_k => 5
) vs
JOIN `ecommerce_intelligence.products` p1 ON vs.product_id = p1.product_id
WHERE vs.product_id != 'PROD001'
ORDER BY distance ASC;

-- 4. Review Sentiment Analysis and Summarization
-- Analyze and summarize customer reviews
SELECT 
    p.product_id,
    p.name,
    p.category,
    COUNT(r.review_id) as total_reviews,
    AVG(r.rating) as avg_rating,
    AI.GENERATE(
        'text-bison@001',
        CONCAT(
            'Summarize the following customer reviews for ', p.name, ' into 3 key points: ',
            STRING_AGG(r.review_text, ' ')
        ),
        300,
        0.5,
        0.8,
        40
    ) AS review_summary,
    AI.GENERATE(
        'text-bison@001',
        CONCAT(
            'Analyze the sentiment of these reviews and provide a sentiment score (1-10): ',
            STRING_AGG(r.review_text, ' ')
        ),
        100,
        0.3,
        0.8,
        40
    ) AS sentiment_analysis
FROM `ecommerce_intelligence.products` p
JOIN `ecommerce_intelligence.reviews` r ON p.product_id = r.product_id
GROUP BY p.product_id, p.name, p.category
HAVING COUNT(r.review_id) >= 2;

-- 5. Demand Forecasting
-- Forecast product demand using AI.FORECAST
SELECT 
    product_id,
    AI.FORECAST(
        daily_sales,
        30,  -- forecast periods
        0.95 -- confidence level
    ) AS demand_forecast
FROM (
    SELECT 
        oi.product_id,
        DATE(o.order_date) as date,
        SUM(oi.quantity) as daily_sales
    FROM `ecommerce_intelligence.orders` o
    JOIN `ecommerce_intelligence.order_items` oi ON o.order_id = oi.order_id
    WHERE o.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    GROUP BY oi.product_id, DATE(o.order_date)
    ORDER BY oi.product_id, date
)
WHERE product_id = 'PROD001';

-- 6. Revenue Forecasting
-- Forecast overall business revenue
SELECT 
    AI.FORECAST(
        daily_revenue,
        30,
        0.95
    ) AS revenue_forecast
FROM (
    SELECT 
        DATE(order_date) as date,
        SUM(total_amount) as daily_revenue
    FROM `ecommerce_intelligence.orders`
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    GROUP BY DATE(order_date)
    ORDER BY date
);

-- 7. Customer Segmentation with AI
-- Use AI to segment customers based on behavior
SELECT 
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    COUNT(o.order_id) as total_orders,
    AVG(o.total_amount) as avg_order_value,
    SUM(o.total_amount) as total_spent,
    AI.GENERATE(
        'text-bison@001',
        CONCAT(
            'Based on the following customer data, classify this customer into a segment (VIP, Regular, Occasional, or New): ',
            'Total orders: ', CAST(COUNT(o.order_id) AS STRING), ', ',
            'Average order value: $', CAST(AVG(o.total_amount) AS STRING), ', ',
            'Total spent: $', CAST(SUM(o.total_amount) AS STRING), ', ',
            'Registration date: ', CAST(u.registration_date AS STRING)
        ),
        100,
        0.3,
        0.8,
        40
    ) AS customer_segment
FROM `ecommerce_intelligence.users` u
LEFT JOIN `ecommerce_intelligence.orders` o ON u.user_id = o.user_id
WHERE u.is_active = true
GROUP BY u.user_id, u.first_name, u.last_name, u.email, u.registration_date;

-- 8. Product Recommendation Engine
-- Generate product recommendations using AI
SELECT 
    u.user_id,
    u.first_name,
    p.product_id,
    p.name,
    p.category,
    p.price,
    AI.GENERATE(
        'text-bison@001',
        CONCAT(
            'Explain why ', p.name, ' would be a good recommendation for ', u.first_name,
            ' based on their purchase history and preferences. Keep it under 100 words.'
        ),
        150,
        0.7,
        0.8,
        40
    ) AS recommendation_reason
FROM `ecommerce_intelligence.users` u
CROSS JOIN `ecommerce_intelligence.products` p
WHERE u.user_id = 'USER001'
AND p.stock_quantity > 0
AND p.product_id NOT IN (
    SELECT DISTINCT oi.product_id
    FROM `ecommerce_intelligence.orders` o
    JOIN `ecommerce_intelligence.order_items` oi ON o.order_id = oi.order_id
    WHERE o.user_id = u.user_id
)
ORDER BY p.rating DESC, p.price ASC
LIMIT 5;

-- 9. Inventory Optimization
-- Use AI to suggest inventory levels
SELECT 
    p.product_id,
    p.name,
    p.stock_quantity,
    AVG(oi.quantity) as avg_order_quantity,
    COUNT(o.order_id) as total_orders,
    AI.GENERATE(
        'text-bison@001',
        CONCAT(
            'Based on current stock of ', CAST(p.stock_quantity AS STRING), ' units, ',
            'average order quantity of ', CAST(AVG(oi.quantity) AS STRING), ' units, ',
            'and ', CAST(COUNT(o.order_id) AS STRING), ' total orders, ',
            'suggest optimal inventory level and reorder strategy for ', p.name, '.'
        ),
        200,
        0.5,
        0.8,
        40
    ) AS inventory_recommendation
FROM `ecommerce_intelligence.products` p
LEFT JOIN `ecommerce_intelligence.order_items` oi ON p.product_id = oi.product_id
LEFT JOIN `ecommerce_intelligence.orders` o ON oi.order_id = o.order_id
WHERE p.stock_quantity > 0
GROUP BY p.product_id, p.name, p.stock_quantity;

-- 10. Marketing Campaign Performance Analysis
-- Analyze marketing campaign effectiveness
SELECT 
    'Marketing Campaign Analysis' as analysis_type,
    AI.GENERATE(
        'text-bison@001',
        CONCAT(
            'Analyze the following e-commerce data and provide 3 key marketing insights: ',
            'Total users: ', CAST(COUNT(DISTINCT u.user_id) AS STRING), ', ',
            'Total orders: ', CAST(COUNT(DISTINCT o.order_id) AS STRING), ', ',
            'Total revenue: $', CAST(SUM(o.total_amount) AS STRING), ', ',
            'Average order value: $', CAST(AVG(o.total_amount) AS STRING), ', ',
            'Customer retention rate: ', CAST(COUNT(DISTINCT o.user_id) * 100.0 / COUNT(DISTINCT u.user_id) AS STRING), '%'
        ),
        300,
        0.6,
        0.8,
        40
    ) AS marketing_insights
FROM `ecommerce_intelligence.users` u
LEFT JOIN `ecommerce_intelligence.orders` o ON u.user_id = o.user_id
WHERE u.is_active = true;
