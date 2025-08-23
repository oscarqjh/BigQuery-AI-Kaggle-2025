#!/usr/bin/env python3
"""
Test script for BigQuery AI functions
"""

import os
from google.cloud import bigquery
from config.bigquery_config import get_bigquery_client

def test_ai_functions():
    """Test BigQuery AI functions with correct syntax"""
    
    client = get_bigquery_client()
    
    print("Testing BigQuery AI functions...")
    
    # Test 1: Simple AI.GENERATE
    try:
        query1 = """
        SELECT AI.GENERATE(
            prompt => 'Hello, how are you?',
            model_params => JSON '{"max_tokens": 50, "temperature": 0.7}'
        ) AS response
        """
        
        print("Testing AI.GENERATE...")
        query_job = client.query(query1)
        results = query_job.result()
        
        for row in results:
            print(f"AI.GENERATE Response: {row.response}")
            break
            
    except Exception as e:
        print(f"AI.GENERATE Error: {e}")
    
    # Test 2: Simple ML.GENERATE_EMBEDDING
    try:
        query2 = """
        SELECT ML.GENERATE_EMBEDDING(
            'This is a test text',
            model => 'textembedding-gecko@001'
        ) AS embedding
        """
        
        print("Testing ML.GENERATE_EMBEDDING...")
        query_job = client.query(query2)
        results = query_job.result()
        
        for row in results:
            print(f"ML.GENERATE_EMBEDDING Response: {len(row.embedding)} dimensions")
            break
            
    except Exception as e:
        print(f"ML.GENERATE_EMBEDDING Error: {e}")
    
    # Test 3: Simple AI.FORECAST (if we have time series data)
    try:
        query3 = """
        SELECT AI.FORECAST(
            [1, 2, 3, 4, 5],
            3
        ) AS forecast
        """
        
        print("Testing AI.FORECAST...")
        query_job = client.query(query3)
        results = query_job.result()
        
        for row in results:
            print(f"AI.FORECAST Response: {row.forecast}")
            break
            
    except Exception as e:
        print(f"AI.FORECAST Error: {e}")

if __name__ == "__main__":
    test_ai_functions()
