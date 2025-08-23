"""
Unit Tests for Smart E-Commerce Intelligence Engine

Tests for all major components of the system.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.ai_engine import AIEngine
from src.marketing_engine import MarketingEngine
from src.vector_search import VectorSearchEngine
from src.forecasting import ForecastingEngine
from src.data_ingestion import DataIngestion

class TestAIEngine(unittest.TestCase):
    """Test cases for AI Engine"""
    
    def setUp(self):
        """Set up test fixtures"""
        with patch('src.ai_engine.get_bigquery_client'):
            self.ai_engine = AIEngine()
    
    def test_generate_text(self):
        """Test text generation"""
        with patch.object(self.ai_engine.client, 'query') as mock_query:
            mock_result = Mock()
            mock_result.result.return_value = [Mock(generated_text="Test generated text")]
            mock_query.return_value = mock_result
            
            result = self.ai_engine.generate_text("Test prompt")
            self.assertEqual(result, "Test generated text")
    
    def test_generate_embedding(self):
        """Test embedding generation"""
        with patch.object(self.ai_engine.client, 'query') as mock_query:
            mock_result = Mock()
            mock_result.result.return_value = [Mock(embedding=[0.1, 0.2, 0.3])]
            mock_query.return_value = mock_result
            
            result = self.ai_engine.generate_embedding("Test text")
            self.assertEqual(result, [0.1, 0.2, 0.3])
    
    def test_analyze_sentiment(self):
        """Test sentiment analysis"""
        with patch.object(self.ai_engine, 'generate_text') as mock_generate:
            mock_generate.return_value = '{"sentiment": "positive", "confidence": 0.8}'
            
            result = self.ai_engine.analyze_sentiment("Great product!")
            self.assertIn('sentiment', result)
            self.assertIn('confidence', result)

class TestMarketingEngine(unittest.TestCase):
    """Test cases for Marketing Engine"""
    
    def setUp(self):
        """Set up test fixtures"""
        with patch('src.marketing_engine.get_bigquery_client'):
            with patch('src.marketing_engine.AIEngine'):
                self.marketing_engine = MarketingEngine()
    
    def test_generate_personalized_email(self):
        """Test personalized email generation"""
        with patch.object(self.marketing_engine, '_get_user_data') as mock_get_user:
            mock_get_user.return_value = {
                'first_name': 'John',
                'last_name': 'Doe',
                'total_orders': 5,
                'avg_order_value': 100.0
            }
            
            with patch.object(self.marketing_engine.ai_engine, 'generate_text') as mock_generate:
                mock_generate.return_value = "Personalized email content"
                
                result = self.marketing_engine.generate_personalized_email("USER001", "recommendation")
                self.assertEqual(result, "Personalized email content")
    
    def test_generate_product_recommendations_email(self):
        """Test product recommendations email generation"""
        with patch.object(self.marketing_engine, '_get_user_preferences') as mock_prefs:
            mock_prefs.return_value = {'top_categories': []}
            
            with patch.object(self.marketing_engine, '_get_recommended_products') as mock_products:
                mock_products.return_value = [
                    {'name': 'Product 1', 'price': 50.0, 'rating': 4.5}
                ]
                
                with patch.object(self.marketing_engine.ai_engine, 'generate_text') as mock_generate:
                    mock_generate.return_value = "Recommendations email"
                    
                    result = self.marketing_engine.generate_product_recommendations_email("USER001")
                    self.assertEqual(result, "Recommendations email")

class TestVectorSearchEngine(unittest.TestCase):
    """Test cases for Vector Search Engine"""
    
    def setUp(self):
        """Set up test fixtures"""
        with patch('src.vector_search.get_bigquery_client'):
            with patch('src.vector_search.AIEngine'):
                self.vector_search = VectorSearchEngine()
    
    def test_find_similar_products(self):
        """Test finding similar products"""
        with patch.object(self.vector_search, '_get_product_embedding') as mock_embedding:
            mock_embedding.return_value = [0.1, 0.2, 0.3]
            
            with patch.object(self.vector_search.client, 'query') as mock_query:
                mock_result = Mock()
                mock_result.result.return_value = [
                    Mock(
                        product_id='PROD002',
                        name='Similar Product',
                        description='Test description',
                        price=50.0,
                        category='electronics',
                        rating=4.5,
                        image_url='test.jpg',
                        stock_quantity=10,
                        distance=0.1
                    )
                ]
                mock_query.return_value = mock_result
                
                result = self.vector_search.find_similar_products("PROD001", top_k=3)
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0]['product_id'], 'PROD002')
    
    def test_search_products_by_text(self):
        """Test text-based product search"""
        with patch.object(self.vector_search.ai_engine, 'generate_embedding') as mock_embedding:
            mock_embedding.return_value = [0.1, 0.2, 0.3]
            
            with patch.object(self.vector_search.client, 'query') as mock_query:
                mock_result = Mock()
                mock_result.result.return_value = []
                mock_query.return_value = mock_result
                
                result = self.vector_search.search_products_by_text("wireless headphones", top_k=5)
                self.assertEqual(result, [])

class TestForecastingEngine(unittest.TestCase):
    """Test cases for Forecasting Engine"""
    
    def setUp(self):
        """Set up test fixtures"""
        with patch('src.forecasting.get_bigquery_client'):
            self.forecasting = ForecastingEngine()
    
    def test_forecast_product_demand(self):
        """Test product demand forecasting"""
        with patch.object(self.forecasting.client, 'query') as mock_query:
            mock_result = Mock()
            mock_result.result.return_value = [
                Mock(forecast_result='{"predictions": [{"value": 10}]}')
            ]
            mock_query.return_value = mock_result
            
            result = self.forecasting.forecast_product_demand("PROD001", forecast_periods=30)
            self.assertIn('identifier', result)
            self.assertEqual(result['identifier'], 'PROD001')
    
    def test_forecast_revenue(self):
        """Test revenue forecasting"""
        with patch.object(self.forecasting.client, 'query') as mock_query:
            mock_result = Mock()
            mock_result.result.return_value = [
                Mock(forecast_result='{"predictions": [{"value": 1000}]}')
            ]
            mock_query.return_value = mock_result
            
            result = self.forecasting.forecast_revenue(forecast_periods=30)
            self.assertIn('identifier', result)
            self.assertEqual(result['identifier'], 'revenue')

class TestDataIngestion(unittest.TestCase):
    """Test cases for Data Ingestion"""
    
    def setUp(self):
        """Set up test fixtures"""
        with patch('src.data_ingestion.get_bigquery_client'):
            with patch('src.data_ingestion.create_dataset_if_not_exists'):
                self.data_ingestion = DataIngestion()
    
    def test_create_tables(self):
        """Test table creation"""
        with patch.object(self.data_ingestion.client, 'create_table') as mock_create:
            mock_create.return_value = None
            
            result = self.data_ingestion.create_tables()
            self.assertTrue(result)
    
    def test_load_sample_data(self):
        """Test sample data loading"""
        with patch.object(self.data_ingestion.client, 'insert_rows_json') as mock_insert:
            mock_insert.return_value = None
            
            result = self.data_ingestion.load_sample_data()
            self.assertTrue(result)

class TestIntegration(unittest.TestCase):
    """Integration tests for the complete system"""
    
    def setUp(self):
        """Set up test fixtures"""
        with patch('src.main.get_bigquery_client'):
            with patch('src.main.AIEngine'):
                with patch('src.main.MarketingEngine'):
                    with patch('src.main.VectorSearchEngine'):
                        with patch('src.main.ForecastingEngine'):
                            with patch('src.main.DataIngestion'):
                                from src.main import ECommerceIntelligenceEngine
                                self.engine = ECommerceIntelligenceEngine()
    
    def test_engine_initialization(self):
        """Test that the engine initializes correctly"""
        self.assertIsNotNone(self.engine.ai_engine)
        self.assertIsNotNone(self.engine.marketing_engine)
        self.assertIsNotNone(self.engine.vector_search)
        self.assertIsNotNone(self.engine.forecasting)
        self.assertIsNotNone(self.engine.data_ingestion)
    
    def test_setup_database(self):
        """Test database setup"""
        with patch.object(self.engine.data_ingestion, 'create_tables') as mock_create:
            with patch.object(self.engine.data_ingestion, 'load_sample_data') as mock_load:
                mock_create.return_value = True
                mock_load.return_value = True
                
                result = self.engine.setup_database()
                self.assertTrue(result)
    
    def test_demonstrate_ai_engine(self):
        """Test AI engine demonstration"""
        with patch.object(self.engine.ai_engine, 'generate_text') as mock_generate:
            mock_generate.return_value = "Generated text"
            
            result = self.engine.demonstrate_ai_engine()
            self.assertIn('generated_text', result)

def run_tests():
    """Run all tests"""
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_suite.addTest(unittest.makeSuite(TestAIEngine))
    test_suite.addTest(unittest.makeSuite(TestMarketingEngine))
    test_suite.addTest(unittest.makeSuite(TestVectorSearchEngine))
    test_suite.addTest(unittest.makeSuite(TestForecastingEngine))
    test_suite.addTest(unittest.makeSuite(TestDataIngestion))
    test_suite.addTest(unittest.makeSuite(TestIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result.wasSuccessful()

if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
