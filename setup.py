#!/usr/bin/env python3
"""
Setup script for Smart E-Commerce Intelligence & Recommendation Engine

This script helps set up the project environment and configuration.
"""

import os
import sys
import subprocess
import json
from pathlib import Path

def print_banner():
    """Print project banner"""
    print("="*70)
    print("🚀 Smart E-Commerce Intelligence & Recommendation Engine")
    print("="*70)
    print("From raw data to personalized insights, powered by BigQuery Generative AI & Vector Search")
    print("="*70)
    print()

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Error: Python 3.8 or higher is required")
        print(f"Current version: {sys.version}")
        sys.exit(1)
    print(f"✅ Python version: {sys.version.split()[0]}")

def install_dependencies():
    """Install required dependencies"""
    print("📦 Installing dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed successfully")
    except subprocess.CalledProcessError:
        print("❌ Error: Failed to install dependencies")
        sys.exit(1)

def setup_environment():
    """Set up environment variables"""
    print("🔧 Setting up environment...")
    
    # Create .env file if it doesn't exist
    env_file = Path(".env")
    if not env_file.exists():
        print("Creating .env file...")
        env_content = """# BigQuery Configuration
BIGQUERY_DATASET=ecommerce_intelligence
BIGQUERY_LOCATION=US
VERTEX_AI_LOCATION=us-central1

# AI Model Configuration
TEXT_MODEL=text-bison@001
EMBEDDING_MODEL=textembedding-gecko@001

# Environment Settings
ENVIRONMENT=development
DEBUG=True
LOG_LEVEL=INFO

# Add your Google Cloud Project ID below
# GOOGLE_CLOUD_PROJECT=your-project-id
"""
        with open(env_file, "w") as f:
            f.write(env_content)
        print("✅ .env file created")
    else:
        print("✅ .env file already exists")

def setup_google_cloud():
    """Set up Google Cloud configuration"""
    print("☁️  Setting up Google Cloud...")
    
    # Check if gcloud is installed
    try:
        subprocess.run(["gcloud", "--version"], capture_output=True, check=True)
        print("✅ Google Cloud SDK is installed")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Google Cloud SDK not found")
        print("Please install Google Cloud SDK: https://cloud.google.com/sdk/docs/install")
        return False
    
    # Check if user is authenticated
    try:
        result = subprocess.run(["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"], 
                              capture_output=True, text=True, check=True)
        if result.stdout.strip():
            print(f"✅ Authenticated as: {result.stdout.strip()}")
        else:
            print("⚠️  Not authenticated with Google Cloud")
            print("Run: gcloud auth login")
            return False
    except subprocess.CalledProcessError:
        print("⚠️  Could not check authentication status")
        return False
    
    return True

def create_sample_config():
    """Create sample configuration files"""
    print("📝 Creating sample configuration...")
    
    # Create config directory if it doesn't exist
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)
    
    # Create sample BigQuery config
    sample_config = """# Sample BigQuery Configuration
# Copy this file to config/bigquery_config.py and update with your settings

import os
from google.cloud import bigquery
from google.auth import default

class BigQueryConfig:
    def __init__(self):
        # Get credentials and project ID
        self.credentials, self.project_id = default()
        
        # BigQuery settings
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'ecommerce_intelligence')
        self.location = os.getenv('BIGQUERY_LOCATION', 'US')
        
        # Initialize BigQuery client
        self.client = bigquery.Client(
            project=self.project_id,
            location=self.location
        )

# Global configuration instance
config = BigQueryConfig()
"""
    
    sample_config_file = config_dir / "bigquery_config.py.example"
    with open(sample_config_file, "w") as f:
        f.write(sample_config)
    
    print("✅ Sample configuration files created")

def run_tests():
    """Run the test suite"""
    print("🧪 Running tests...")
    try:
        subprocess.check_call([sys.executable, "-m", "pytest", "tests/", "-v"])
        print("✅ All tests passed")
    except subprocess.CalledProcessError:
        print("❌ Some tests failed")
        return False
    return True

def print_next_steps():
    """Print next steps for the user"""
    print("\n" + "="*70)
    print("🎉 Setup completed successfully!")
    print("="*70)
    print("\nNext steps:")
    print("1. Configure your Google Cloud Project:")
    print("   - Set your project ID in .env file")
    print("   - Enable BigQuery and Vertex AI APIs")
    print("   - Set up authentication")
    print()
    print("2. Run the application:")
    print("   python src/main.py")
    print()
    print("3. Explore the notebooks:")
    print("   jupyter notebook notebooks/01_data_exploration.ipynb")
    print()
    print("4. Check the documentation:")
    print("   - README.md for detailed instructions")
    print("   - data/sql/ for example queries")
    print()
    print("5. Run tests:")
    print("   python -m pytest tests/")
    print("\n" + "="*70)

def main():
    """Main setup function"""
    print_banner()
    
    # Check Python version
    check_python_version()
    
    # Install dependencies
    install_dependencies()
    
    # Setup environment
    setup_environment()
    
    # Setup Google Cloud
    gcloud_ok = setup_google_cloud()
    
    # Create sample configuration
    create_sample_config()
    
    # Run tests if Google Cloud is set up
    if gcloud_ok:
        run_tests()
    else:
        print("⚠️  Skipping tests due to Google Cloud setup issues")
    
    # Print next steps
    print_next_steps()

if __name__ == "__main__":
    main()
