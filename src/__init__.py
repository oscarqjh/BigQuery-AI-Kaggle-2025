"""
Smart E-Commerce Intelligence & Recommendation Engine

A comprehensive AI-driven solution for e-commerce platforms using BigQuery Generative AI & Vector Search.
"""

__version__ = "1.0.0"
__author__ = "E-Commerce Intelligence Team"
__description__ = "AI-powered e-commerce intelligence and recommendation engine"

from .ai_engine import AIEngine
from .marketing_engine import MarketingEngine
from .vector_search import VectorSearchEngine
from .forecasting import ForecastingEngine
from .data_ingestion import DataIngestion

__all__ = [
    'AIEngine',
    'MarketingEngine', 
    'VectorSearchEngine',
    'ForecastingEngine',
    'DataIngestion'
]
