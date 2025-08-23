"""
Project Settings for Smart E-Commerce Intelligence Engine
"""

import os
from typing import Dict, Any

# Environment settings
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'

# Logging settings
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# AI Model settings
AI_MODELS = {
    'text_generation': {
        'model': 'text-bison@001',
        'max_tokens': 1024,
        'temperature': 0.7,
        'top_p': 0.8,
        'top_k': 40
    },
    'embedding': {
        'model': 'textembedding-gecko@001',
        'dimension': 768
    },
    'forecasting': {
        'model': 'ai_forecast',
        'forecast_periods': 30,
        'confidence_level': 0.95
    }
}

# Vector Search settings
VECTOR_SEARCH_CONFIG = {
    'distance_type': 'COSINE',
    'top_k': 10,
    'min_score': 0.7
}

# Marketing Engine settings
MARKETING_CONFIG = {
    'email_templates': {
        'welcome': 'Welcome to our store! We noticed you recently purchased {product}.',
        'recommendation': 'Based on your purchase of {product}, you might like {recommended_product}.',
        'discount': 'Special {discount}% discount on {category} for you!',
        'abandoned_cart': 'Don\'t forget about the items in your cart: {items}'
    },
    'personalization_factors': [
        'purchase_history',
        'browsing_behavior',
        'demographics',
        'seasonal_preferences',
        'price_sensitivity'
    ]
}

# Product Recommendation settings
RECOMMENDATION_CONFIG = {
    'similarity_threshold': 0.8,
    'max_recommendations': 5,
    'categories_to_consider': [
        'electronics',
        'clothing',
        'home_garden',
        'sports_outdoors',
        'books_movies'
    ],
    'exclude_out_of_stock': True
}

# Review Analysis settings
REVIEW_ANALYSIS_CONFIG = {
    'sentiment_thresholds': {
        'positive': 0.6,
        'negative': 0.4
    },
    'summary_length': 3,
    'key_topics': [
        'quality',
        'price',
        'delivery',
        'customer_service',
        'design'
    ]
}

# Forecasting settings
FORECASTING_CONFIG = {
    'default_periods': 30,
    'seasonality': True,
    'trend': True,
    'confidence_intervals': [0.8, 0.95]
}

# Data Processing settings
DATA_PROCESSING_CONFIG = {
    'batch_size': 1000,
    'max_workers': 4,
    'chunk_size': 100
}

# Cache settings
CACHE_CONFIG = {
    'enabled': True,
    'ttl': 3600,  # 1 hour
    'max_size': 1000
}

# API Rate limiting
RATE_LIMIT_CONFIG = {
    'requests_per_minute': 60,
    'requests_per_hour': 1000
}

# Error handling
ERROR_CONFIG = {
    'max_retries': 3,
    'retry_delay': 1,  # seconds
    'exponential_backoff': True
}

# Monitoring and alerting
MONITORING_CONFIG = {
    'enabled': True,
    'metrics': [
        'api_latency',
        'error_rate',
        'recommendation_accuracy',
        'forecast_accuracy'
    ],
    'alerts': {
        'error_threshold': 0.05,  # 5%
        'latency_threshold': 5000  # 5 seconds
    }
}

def get_config() -> Dict[str, Any]:
    """Get complete configuration dictionary"""
    return {
        'environment': ENVIRONMENT,
        'debug': DEBUG,
        'ai_models': AI_MODELS,
        'vector_search': VECTOR_SEARCH_CONFIG,
        'marketing': MARKETING_CONFIG,
        'recommendation': RECOMMENDATION_CONFIG,
        'review_analysis': REVIEW_ANALYSIS_CONFIG,
        'forecasting': FORECASTING_CONFIG,
        'data_processing': DATA_PROCESSING_CONFIG,
        'cache': CACHE_CONFIG,
        'rate_limit': RATE_LIMIT_CONFIG,
        'error_handling': ERROR_CONFIG,
        'monitoring': MONITORING_CONFIG
    }

def get_ai_model_config(model_type: str) -> Dict[str, Any]:
    """Get AI model configuration for specific type"""
    return AI_MODELS.get(model_type, {})

def get_marketing_config() -> Dict[str, Any]:
    """Get marketing engine configuration"""
    return MARKETING_CONFIG

def get_recommendation_config() -> Dict[str, Any]:
    """Get recommendation engine configuration"""
    return RECOMMENDATION_CONFIG
