#!/usr/bin/env python3
"""
Authentication Setup Script for Smart E-Commerce Intelligence Engine
"""

import os
import sys
from config.auth_helper import AuthHelper

def main():
    print("🔐 Google Cloud Authentication Setup")
    print("=" * 50)
    
    while True:
        print("\nChoose authentication method:")
        print("1. Service Account Key File")
        print("2. Service Account JSON String")
        print("3. API Key")
        print("4. Default Application Credentials")
        print("5. Test Current Authentication")
        print("6. Create .env Template")
        print("7. Exit")
        
        choice = input("\nEnter your choice (1-7): ").strip()
        
        if choice == "1":
            setup_service_account_file()
        elif choice == "2":
            setup_service_account_json()
        elif choice == "3":
            setup_api_key()
        elif choice == "4":
            setup_default_credentials()
        elif choice == "5":
            test_current_auth()
        elif choice == "6":
            create_env_template()
        elif choice == "7":
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

def setup_service_account_file():
    """Set up authentication with service account key file"""
    print("\n📁 Service Account Key File Setup")
    print("-" * 30)
    
    key_file_path = input("Enter path to your service account JSON key file: ").strip()
    
    if not key_file_path:
        print("❌ No file path provided")
        return
    
    try:
        project_id = AuthHelper.setup_service_account_from_file(key_file_path)
        print(f"\n✅ Authentication set up successfully!")
        print(f"   Project ID: {project_id}")
        print(f"   Key file: {key_file_path}")
    except Exception as e:
        print(f"❌ Error: {e}")

def setup_service_account_json():
    """Set up authentication with service account JSON string"""
    print("\n📄 Service Account JSON Setup")
    print("-" * 30)
    print("Paste your service account JSON content below (press Enter twice when done):")
    
    lines = []
    while True:
        line = input()
        if line == "" and lines and lines[-1] == "":
            break
        lines.append(line)
    
    service_account_json = "\n".join(lines[:-1])  # Remove the last empty line
    
    if not service_account_json.strip():
        print("❌ No JSON content provided")
        return
    
    try:
        project_id = AuthHelper.setup_service_account_from_json(service_account_json)
        print(f"\n✅ Authentication set up successfully!")
        print(f"   Project ID: {project_id}")
    except Exception as e:
        print(f"❌ Error: {e}")

def setup_api_key():
    """Set up authentication with API key"""
    print("\n🔑 API Key Setup")
    print("-" * 30)
    print("Note: API keys are typically used for specific API endpoints, not BigQuery")
    
    api_key = input("Enter your Google Cloud API key: ").strip()
    
    if not api_key:
        print("❌ No API key provided")
        return
    
    try:
        AuthHelper.setup_api_key(api_key)
        print(f"\n✅ API key set up successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

def setup_default_credentials():
    """Set up default application credentials"""
    print("\n👤 Default Application Credentials Setup")
    print("-" * 40)
    print("This requires you to have run 'gcloud auth application-default login'")
    
    try:
        project_id = AuthHelper.setup_default_credentials()
        print(f"\n✅ Default credentials set up successfully!")
        print(f"   Project ID: {project_id}")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nTo fix this, run:")
        print("gcloud auth application-default login")

def test_current_auth():
    """Test current authentication setup"""
    print("\n🧪 Testing Current Authentication")
    print("-" * 35)
    
    success = AuthHelper.test_authentication()
    
    if success:
        print("\n✅ Authentication is working correctly!")
    else:
        print("\n❌ Authentication failed. Please set up authentication first.")

def create_env_template():
    """Create .env template file"""
    print("\n📝 Creating .env Template")
    print("-" * 25)
    
    try:
        AuthHelper.create_env_template()
        print("\n✅ .env.template created successfully!")
        print("   Copy .env.template to .env and fill in your values")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
