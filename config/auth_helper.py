"""
Authentication Helper for Google Cloud Services
Provides easy setup for different authentication methods
"""

import os
import json
from typing import Optional
from google.auth import default
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError

class AuthHelper:
    """Helper class for Google Cloud authentication setup"""
    
    @staticmethod
    def setup_service_account_from_file(key_file_path: str, project_id: Optional[str] = None):
        """
        Set up authentication using a service account key file
        
        Args:
            key_file_path: Path to the service account JSON key file
            project_id: Optional project ID override
        """
        if not os.path.exists(key_file_path):
            raise FileNotFoundError(f"Service account key file not found: {key_file_path}")
        
        # Set environment variable
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file_path
        
        # Verify the credentials work
        try:
            credentials = service_account.Credentials.from_service_account_file(key_file_path)
            actual_project_id = project_id or credentials.project_id
            print(f"✅ Service account authentication set up successfully")
            print(f"   Project ID: {actual_project_id}")
            print(f"   Service account: {credentials.service_account_email}")
            return actual_project_id
        except Exception as e:
            raise Exception(f"Failed to load service account credentials: {e}")
    
    @staticmethod
    def setup_service_account_from_json(service_account_json: str, project_id: Optional[str] = None):
        """
        Set up authentication using service account JSON string
        
        Args:
            service_account_json: JSON string containing service account credentials
            project_id: Optional project ID override
        """
        try:
            service_account_info = json.loads(service_account_json)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            actual_project_id = project_id or credentials.project_id
            
            # Set environment variable
            os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'] = service_account_json
            
            print(f"✅ Service account authentication set up successfully")
            print(f"   Project ID: {actual_project_id}")
            print(f"   Service account: {credentials.service_account_email}")
            return actual_project_id
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON format: {e}")
        except Exception as e:
            raise Exception(f"Failed to load service account credentials: {e}")
    
    @staticmethod
    def setup_api_key(api_key: str):
        """
        Set up API key for specific API calls
        
        Args:
            api_key: Google Cloud API key
        """
        os.environ['GOOGLE_API_KEY'] = api_key
        print(f"✅ API key set up successfully")
        print(f"   Note: API keys are typically used for specific API endpoints, not BigQuery")
    
    @staticmethod
    def setup_default_credentials():
        """
        Set up default application credentials (requires gcloud auth)
        """
        try:
            credentials, project_id = default()
            print(f"✅ Default credentials set up successfully")
            print(f"   Project ID: {project_id}")
            return project_id
        except DefaultCredentialsError:
            raise Exception(
                "Default credentials not found. Please run:\n"
                "gcloud auth application-default login"
            )
    
    @staticmethod
    def test_authentication():
        """
        Test the current authentication setup
        """
        try:
            credentials, project_id = default()
            print(f"✅ Authentication test successful")
            print(f"   Project ID: {project_id}")
            print(f"   Credentials type: {type(credentials).__name__}")
            
            if hasattr(credentials, 'service_account_email'):
                print(f"   Service account: {credentials.service_account_email}")
            
            return True
        except Exception as e:
            print(f"❌ Authentication test failed: {e}")
            return False
    
    @staticmethod
    def create_env_template():
        """
        Create a template .env file with authentication options
        """
        template = """# Google Cloud Authentication Options
# Choose ONE of the following methods:

# Method 1: Service Account Key File
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json

# Method 2: Service Account JSON String
# GOOGLE_SERVICE_ACCOUNT_JSON={"type": "service_account", "project_id": "your-project", ...}

# Method 3: API Key (for specific API calls)
# GOOGLE_API_KEY=your-api-key-here

# Project Configuration
GOOGLE_CLOUD_PROJECT_ID=your-project-id
BIGQUERY_DATASET=ecommerce_intelligence
BIGQUERY_LOCATION=US
VERTEX_AI_LOCATION=us-central1
STORAGE_BUCKET=your-project-id-ecommerce-images

# AI Model Configuration
TEXT_MODEL=text-bison@001
EMBEDDING_MODEL=textembedding-gecko@001
"""
        
        with open('.env.template', 'w') as f:
            f.write(template)
        
        print("✅ Created .env.template file")
        print("   Copy this file to .env and fill in your values")

# Convenience functions
def setup_auth_with_key_file(key_file_path: str, project_id: Optional[str] = None):
    """Quick setup with service account key file"""
    return AuthHelper.setup_service_account_from_file(key_file_path, project_id)

def setup_auth_with_json(service_account_json: str, project_id: Optional[str] = None):
    """Quick setup with service account JSON"""
    return AuthHelper.setup_service_account_from_json(service_account_json, project_id)

def setup_auth_with_api_key(api_key: str):
    """Quick setup with API key"""
    return AuthHelper.setup_api_key(api_key)

def test_auth():
    """Quick authentication test"""
    return AuthHelper.test_authentication()
