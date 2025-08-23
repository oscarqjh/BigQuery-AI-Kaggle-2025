"""
Data Ingestion for Smart E-Commerce Intelligence

Handles loading and processing data into BigQuery tables.
"""

import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from google.cloud import bigquery
from config.bigquery_config import get_bigquery_client, config, create_dataset_if_not_exists

logger = logging.getLogger(__name__)

class DataIngestion:
    """Data ingestion engine for loading data into BigQuery"""
    
    def __init__(self):
        self.client = get_bigquery_client()
        create_dataset_if_not_exists()
    
    def create_tables(self) -> bool:
        """
        Create all required tables in BigQuery
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create products table
            self._create_products_table()
            
            # Create users table
            self._create_users_table()
            
            # Create orders table
            self._create_orders_table()
            
            # Create order items table
            self._create_order_items_table()
            
            # Create reviews table
            self._create_reviews_table()
            
            # Create user behavior table
            self._create_user_behavior_table()
            
            # Create sales data table
            self._create_sales_data_table()
            
            logger.info("All tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            return False
    
    def load_sample_data(self) -> bool:
        """
        Load sample data into all tables (only if tables are empty)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if data already exists and only load if tables are empty
            if self._table_has_data(config.products_table):
                logger.info("Products table already has data, skipping sample data load")
            else:
                self._load_sample_products()
            
            if self._table_has_data(config.users_table):
                logger.info("Users table already has data, skipping sample data load")
            else:
                self._load_sample_users()
            
            if self._table_has_data(config.orders_table):
                logger.info("Orders table already has data, skipping sample data load")
            else:
                self._load_sample_orders()
            
            if self._table_has_data(config.reviews_table):
                logger.info("Reviews table already has data, skipping sample data load")
            else:
                self._load_sample_reviews()
            
            if self._table_has_data(config.user_behavior_table):
                logger.info("User behavior table already has data, skipping sample data load")
            else:
                self._load_sample_user_behavior()
            
            logger.info("Sample data loading completed (skipped existing data)")
            return True
            
        except Exception as e:
            logger.error(f"Error loading sample data: {e}")
            return False
    
    def _table_has_data(self, table_id: str) -> bool:
        """
        Check if a table has any data
        
        Args:
            table_id: Full table ID (project.dataset.table)
            
        Returns:
            True if table has data, False otherwise
        """
        try:
            query = f"SELECT COUNT(*) as count FROM `{table_id}` LIMIT 1"
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return row.count > 0
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if table {table_id} has data: {e}")
            return False
    
    def _create_products_table(self):
        """Create products table"""
        schema = [
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("brand", "STRING"),
            bigquery.SchemaField("price", "FLOAT64"),
            bigquery.SchemaField("rating", "FLOAT64"),
            bigquery.SchemaField("stock_quantity", "INTEGER"),
            bigquery.SchemaField("image_url", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP")
        ]
        
        table = bigquery.Table(config.products_table, schema=schema)
        self.client.create_table(table, exists_ok=True)
        logger.info(f"Created products table: {config.products_table}")
    
    def _create_users_table(self):
        """Create users table"""
        schema = [
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("first_name", "STRING"),
            bigquery.SchemaField("last_name", "STRING"),
            bigquery.SchemaField("demographics", "STRING"),
            bigquery.SchemaField("user_segment", "STRING"),
            bigquery.SchemaField("registration_date", "TIMESTAMP"),
            bigquery.SchemaField("last_login", "TIMESTAMP"),
            bigquery.SchemaField("is_active", "BOOLEAN")
        ]
        
        table = bigquery.Table(config.users_table, schema=schema)
        self.client.create_table(table, exists_ok=True)
        logger.info(f"Created users table: {config.users_table}")
    
    def _create_orders_table(self):
        """Create orders table"""
        schema = [
            bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("order_date", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("total_amount", "FLOAT64"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("shipping_address", "STRING"),
            bigquery.SchemaField("payment_method", "STRING")
        ]
        
        table = bigquery.Table(config.orders_table, schema=schema)
        self.client.create_table(table, exists_ok=True)
        logger.info(f"Created orders table: {config.orders_table}")
    
    def _create_order_items_table(self):
        """Create order items table"""
        schema = [
            bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("price", "FLOAT64"),
            bigquery.SchemaField("total_price", "FLOAT64")
        ]
        
        table_id = f"{config.dataset_ref}.order_items"
        table = bigquery.Table(table_id, schema=schema)
        self.client.create_table(table, exists_ok=True)
        logger.info(f"Created order items table: {table_id}")
    
    def _create_reviews_table(self):
        """Create reviews table"""
        schema = [
            bigquery.SchemaField("review_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rating", "INTEGER"),
            bigquery.SchemaField("review_text", "STRING"),
            bigquery.SchemaField("review_date", "TIMESTAMP"),
            bigquery.SchemaField("helpful_votes", "INTEGER")
        ]
        
        table = bigquery.Table(config.reviews_table, schema=schema)
        self.client.create_table(table, exists_ok=True)
        logger.info(f"Created reviews table: {config.reviews_table}")
    
    def _create_user_behavior_table(self):
        """Create user behavior table"""
        schema = [
            bigquery.SchemaField("behavior_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("action_type", "STRING"),  # view, add_to_cart, purchase, etc.
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("session_id", "STRING"),
            bigquery.SchemaField("quantity", "INTEGER")
        ]
        
        table = bigquery.Table(config.user_behavior_table, schema=schema)
        self.client.create_table(table, exists_ok=True)
        logger.info(f"Created user behavior table: {config.user_behavior_table}")
    
    def _create_sales_data_table(self):
        """Create sales data table"""
        schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("quantity_sold", "INTEGER"),
            bigquery.SchemaField("revenue", "FLOAT64"),
            bigquery.SchemaField("orders_count", "INTEGER")
        ]
        
        table = bigquery.Table(config.sales_data_table, schema=schema)
        self.client.create_table(table, exists_ok=True)
        logger.info(f"Created sales data table: {config.sales_data_table}")
    
    def _load_sample_products(self):
        """Load sample product data"""
        sample_products = [
            {
                "product_id": "PROD001",
                "name": "Wireless Bluetooth Headphones",
                "description": "High-quality wireless headphones with noise cancellation and 30-hour battery life",
                "category": "electronics",
                "brand": "TechAudio",
                "price": 89.99,
                "rating": 4.5,
                "stock_quantity": 150,
                "image_url": "https://example.com/headphones.jpg",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            },
            {
                "product_id": "PROD002",
                "name": "Organic Cotton T-Shirt",
                "description": "Comfortable organic cotton t-shirt available in multiple colors and sizes",
                "category": "clothing",
                "brand": "EcoWear",
                "price": 24.99,
                "rating": 4.2,
                "stock_quantity": 300,
                "image_url": "https://example.com/tshirt.jpg",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            },
            {
                "product_id": "PROD003",
                "name": "Smart Fitness Watch",
                "description": "Advanced fitness tracking watch with heart rate monitor and GPS",
                "category": "electronics",
                "brand": "FitTech",
                "price": 199.99,
                "rating": 4.7,
                "stock_quantity": 75,
                "image_url": "https://example.com/watch.jpg",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            },
            {
                "product_id": "PROD004",
                "name": "Stainless Steel Water Bottle",
                "description": "Insulated water bottle keeps drinks cold for 24 hours or hot for 12 hours",
                "category": "home_garden",
                "brand": "HydraLife",
                "price": 34.99,
                "rating": 4.6,
                "stock_quantity": 200,
                "image_url": "https://example.com/bottle.jpg",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            },
            {
                "product_id": "PROD005",
                "name": "Yoga Mat Premium",
                "description": "Non-slip yoga mat made from eco-friendly materials with carrying strap",
                "category": "sports_outdoors",
                "brand": "ZenFit",
                "price": 49.99,
                "rating": 4.4,
                "stock_quantity": 120,
                "image_url": "https://example.com/yogamat.jpg",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
        ]
        
        errors = self.client.insert_rows_json(config.products_table, sample_products)
        if errors:
            logger.error(f"Error inserting products: {errors}")
        else:
            logger.info(f"Loaded {len(sample_products)} sample products")
    
    def _load_sample_users(self):
        """Load sample user data"""
        sample_users = [
            {
                "user_id": "USER001",
                "email": "john.doe@example.com",
                "first_name": "John",
                "last_name": "Doe",
                "demographics": "male_25_34",
                "user_segment": "active",
                "registration_date": (datetime.now() - timedelta(days=365)).isoformat(),
                "last_login": (datetime.now() - timedelta(hours=2)).isoformat(),
                "is_active": True
            },
            {
                "user_id": "USER002",
                "email": "jane.smith@example.com",
                "first_name": "Jane",
                "last_name": "Smith",
                "demographics": "female_35_44",
                "user_segment": "premium",
                "registration_date": (datetime.now() - timedelta(days=180)).isoformat(),
                "last_login": (datetime.now() - timedelta(days=1)).isoformat(),
                "is_active": True
            },
            {
                "user_id": "USER003",
                "email": "mike.johnson@example.com",
                "first_name": "Mike",
                "last_name": "Johnson",
                "demographics": "male_18_24",
                "user_segment": "new",
                "registration_date": (datetime.now() - timedelta(days=30)).isoformat(),
                "last_login": (datetime.now() - timedelta(hours=5)).isoformat(),
                "is_active": True
            }
        ]
        
        errors = self.client.insert_rows_json(config.users_table, sample_users)
        if errors:
            logger.error(f"Error inserting users: {errors}")
        else:
            logger.info(f"Loaded {len(sample_users)} sample users")
    
    def _load_sample_orders(self):
        """Load sample order data"""
        sample_orders = [
            {
                "order_id": "ORD001",
                "user_id": "USER001",
                "order_date": (datetime.now() - timedelta(days=5)).isoformat(),
                "total_amount": 114.98,
                "status": "delivered",
                "shipping_address": "123 Main St, City, State 12345",
                "payment_method": "credit_card"
            },
            {
                "order_id": "ORD002",
                "user_id": "USER002",
                "order_date": (datetime.now() - timedelta(days=3)).isoformat(),
                "total_amount": 199.99,
                "status": "shipped",
                "shipping_address": "456 Oak Ave, City, State 12345",
                "payment_method": "paypal"
            },
            {
                "order_id": "ORD003",
                "user_id": "USER001",
                "order_date": (datetime.now() - timedelta(days=1)).isoformat(),
                "total_amount": 49.99,
                "status": "processing",
                "shipping_address": "123 Main St, City, State 12345",
                "payment_method": "credit_card"
            }
        ]
        
        errors = self.client.insert_rows_json(config.orders_table, sample_orders)
        if errors:
            logger.error(f"Error inserting orders: {errors}")
        else:
            logger.info(f"Loaded {len(sample_orders)} sample orders")
        
        # Load order items
        sample_order_items = [
            {"order_id": "ORD001", "product_id": "PROD001", "quantity": 1, "price": 89.99, "total_price": 89.99},
            {"order_id": "ORD001", "product_id": "PROD002", "quantity": 1, "price": 24.99, "total_price": 24.99},
            {"order_id": "ORD002", "product_id": "PROD003", "quantity": 1, "price": 199.99, "total_price": 199.99},
            {"order_id": "ORD003", "product_id": "PROD005", "quantity": 1, "price": 49.99, "total_price": 49.99}
        ]
        
        order_items_table = f"{config.dataset_ref}.order_items"
        errors = self.client.insert_rows_json(order_items_table, sample_order_items)
        if errors:
            logger.error(f"Error inserting order items: {errors}")
        else:
            logger.info(f"Loaded {len(sample_order_items)} sample order items")
    
    def _load_sample_reviews(self):
        """Load sample review data"""
        sample_reviews = [
            {
                "review_id": "REV001",
                "product_id": "PROD001",
                "user_id": "USER001",
                "rating": 5,
                "review_text": "Excellent sound quality and battery life. The noise cancellation is amazing!",
                "review_date": (datetime.now() - timedelta(days=3)).isoformat(),
                "helpful_votes": 12
            },
            {
                "review_id": "REV002",
                "product_id": "PROD001",
                "user_id": "USER002",
                "rating": 4,
                "review_text": "Great headphones, very comfortable for long listening sessions.",
                "review_date": (datetime.now() - timedelta(days=7)).isoformat(),
                "helpful_votes": 8
            },
            {
                "review_id": "REV003",
                "product_id": "PROD002",
                "user_id": "USER001",
                "rating": 4,
                "review_text": "Soft and comfortable fabric. Perfect fit and great quality for the price.",
                "review_date": (datetime.now() - timedelta(days=2)).isoformat(),
                "helpful_votes": 5
            },
            {
                "review_id": "REV004",
                "product_id": "PROD003",
                "user_id": "USER002",
                "rating": 5,
                "review_text": "Outstanding fitness tracker! The GPS accuracy is spot on and battery lasts a week.",
                "review_date": (datetime.now() - timedelta(days=1)).isoformat(),
                "helpful_votes": 15
            }
        ]
        
        errors = self.client.insert_rows_json(config.reviews_table, sample_reviews)
        if errors:
            logger.error(f"Error inserting reviews: {errors}")
        else:
            logger.info(f"Loaded {len(sample_reviews)} sample reviews")
    
    def _load_sample_user_behavior(self):
        """Load sample user behavior data"""
        sample_behavior = [
            {
                "behavior_id": "BEH001",
                "user_id": "USER001",
                "product_id": "PROD001",
                "action_type": "view",
                "timestamp": (datetime.now() - timedelta(days=10)).isoformat(),
                "session_id": "SESS001",
                "quantity": None
            },
            {
                "behavior_id": "BEH002",
                "user_id": "USER001",
                "product_id": "PROD001",
                "action_type": "add_to_cart",
                "timestamp": (datetime.now() - timedelta(days=9)).isoformat(),
                "session_id": "SESS001",
                "quantity": 1
            },
            {
                "behavior_id": "BEH003",
                "user_id": "USER001",
                "product_id": "PROD002",
                "action_type": "view",
                "timestamp": (datetime.now() - timedelta(days=8)).isoformat(),
                "session_id": "SESS002",
                "quantity": None
            },
            {
                "behavior_id": "BEH004",
                "user_id": "USER002",
                "product_id": "PROD003",
                "action_type": "view",
                "timestamp": (datetime.now() - timedelta(days=5)).isoformat(),
                "session_id": "SESS003",
                "quantity": None
            },
            {
                "behavior_id": "BEH005",
                "user_id": "USER002",
                "product_id": "PROD003",
                "action_type": "purchase",
                "timestamp": (datetime.now() - timedelta(days=4)).isoformat(),
                "session_id": "SESS003",
                "quantity": 1
            }
        ]
        
        errors = self.client.insert_rows_json(config.user_behavior_table, sample_behavior)
        if errors:
            logger.error(f"Error inserting user behavior: {errors}")
        else:
            logger.info(f"Loaded {len(sample_behavior)} sample user behavior records")
    
    def load_data_from_csv(self, table_name: str, csv_file_path: str) -> bool:
        """
        Load data from CSV file into BigQuery table
        
        Args:
            table_name: Name of the table to load data into
            csv_file_path: Path to the CSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Read CSV file
            df = pd.read_csv(csv_file_path)
            
            # Get table reference
            table_ref = f"{config.dataset_ref}.{table_name}"
            
            # Load data into BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True
            )
            
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()
            
            logger.info(f"Successfully loaded {len(df)} rows into {table_ref}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data from CSV: {e}")
            return False
    
    def load_data_from_dataframe(self, table_name: str, df: pd.DataFrame) -> bool:
        """
        Load data from pandas DataFrame into BigQuery table
        
        Args:
            table_name: Name of the table to load data into
            df: Pandas DataFrame to load
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get table reference
            table_ref = f"{config.dataset_ref}.{table_name}"
            
            # Load data into BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True
            )
            
            job = self.client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()
            
            logger.info(f"Successfully loaded {len(df)} rows into {table_ref}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data from DataFrame: {e}")
            return False
