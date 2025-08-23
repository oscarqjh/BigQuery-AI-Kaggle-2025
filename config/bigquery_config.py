"""
BigQuery Configuration for Smart E-Commerce Intelligence Engine
"""

import os
import json
from typing import Optional, Union
from google.cloud import bigquery
from google.auth import default
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError

class BigQueryConfig:
    """Configuration class for BigQuery settings"""
    
    def __init__(self):
        # Get credentials and project ID
        self.credentials, self.project_id = self._get_credentials()
        
        # BigQuery settings
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'ecommerce_intelligence')
        self.location = os.getenv('BIGQUERY_LOCATION', 'US')
        
        # Vertex AI settings
        self.vertex_ai_location = os.getenv('VERTEX_AI_LOCATION', 'us-central1')
        
        # Storage settings
        self.storage_bucket = os.getenv('STORAGE_BUCKET', f'{self.project_id}-ecommerce-images')
        
        # AI Model settings
        self.text_model = os.getenv('TEXT_MODEL', 'text-bison@001')
        self.embedding_model = os.getenv('EMBEDDING_MODEL', 'textembedding-gecko@001')
        
        # Vector search settings
        self.vector_index_name = 'product_embeddings_index'
        self.embedding_dimension = 768
        
        # Initialize BigQuery client
        self.client = bigquery.Client(
            project=self.project_id,
            location=self.location,
            credentials=self.credentials
        )
    
    def _get_credentials(self) -> tuple[Credentials, str]:
        """
        Get credentials using the following priority:
        1. Service account key file (GOOGLE_APPLICATION_CREDENTIALS)
        2. Service account JSON string (GOOGLE_SERVICE_ACCOUNT_JSON)
        3. API key (GOOGLE_API_KEY) - for specific API calls
        4. Default application credentials
        """
        from dotenv import load_dotenv
        load_dotenv()
        # Check for service account key file
        service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if service_account_path and os.path.exists(service_account_path):
            print(f"Using service account credentials from: {service_account_path}")
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    service_account_path
                )
                project_id = credentials.project_id
                return credentials, project_id
            except Exception as e:
                print(f"Error loading service account file: {e}")
        
        # Check for service account JSON string
        service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        print(service_account_json)
        if service_account_json:
            print("Using service account JSON from environment variable")
            try:
                service_account_info = json.loads(service_account_json)
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info
                )
                project_id = credentials.project_id
                return credentials, project_id
            except Exception as e:
                print(f"Error loading service account JSON: {e}")
        
        # Check for API key (note: API keys are typically for specific APIs, not BigQuery)
        api_key = os.getenv('GOOGLE_API_KEY')
        if api_key:
            print("API key found - using default credentials (API key for specific API calls)")
            # API keys are typically used for specific API endpoints, not for BigQuery client
            # You would use this for specific API calls that support API key authentication
        
        # Fallback to default credentials
        try:
            print("Using default application credentials")
            return default()
        except DefaultCredentialsError:
            raise Exception(
                "No valid credentials found. Please set one of:\n"
                "1. GOOGLE_APPLICATION_CREDENTIALS (path to service account key file)\n"
                "2. GOOGLE_SERVICE_ACCOUNT_JSON (service account JSON string)\n"
                "3. Run 'gcloud auth application-default login' for default credentials"
            )
    
    @property
    def dataset_ref(self):
        """Get dataset reference"""
        return f"{self.project_id}.{self.dataset_id}"
    
    @property
    def products_table(self):
        """Get products table reference"""
        return f"{self.dataset_ref}.products"
    
    @property
    def users_table(self):
        """Get users table reference"""
        return f"{self.dataset_ref}.users"
    
    @property
    def orders_table(self):
        """Get orders table reference"""
        return f"{self.dataset_ref}.orders"
    
    @property
    def reviews_table(self):
        """Get reviews table reference"""
        return f"{self.dataset_ref}.reviews"
    
    @property
    def user_behavior_table(self):
        """Get user behavior table reference"""
        return f"{self.dataset_ref}.user_behavior"
    
    @property
    def sales_data_table(self):
        """Get sales data table reference"""
        return f"{self.dataset_ref}.sales_data"
    
    @property
    def product_images_table(self):
        """Get product images table reference"""
        return f"{self.dataset_ref}.product_images"

# Global configuration instance
config = BigQueryConfig()

def get_bigquery_client() -> bigquery.Client:
    """Get BigQuery client instance"""
    return config.client

def get_dataset_ref() -> str:
    """Get dataset reference"""
    return config.dataset_ref

def create_dataset_if_not_exists():
    """Create the dataset if it doesn't exist"""
    dataset_ref = config.client.dataset(config.dataset_id)
    
    try:
        config.client.get_dataset(dataset_ref)
        print(f"Dataset {config.dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = config.location
        dataset = config.client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {config.project_id}.{config.dataset_id}")
