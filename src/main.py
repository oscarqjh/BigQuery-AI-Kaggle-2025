"""
Main Application for Smart E-Commerce Intelligence & Recommendation Engine

Demonstrates the complete pipeline with all components working together.
"""

import logging
import sys
from typing import Dict, Any
from src.ai_engine_simple import SimpleAIEngine as AIEngine
from src.marketing_engine import MarketingEngine
from src.vector_search import VectorSearchEngine
from src.forecasting_simple import SimpleForecastingEngine as ForecastingEngine
from src.data_ingestion import DataIngestion
from config.bigquery_config import config
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ECommerceIntelligenceEngine:
    """Main engine that orchestrates all components"""
    
    def __init__(self):
        self.ai_engine = AIEngine()
        self.marketing_engine = MarketingEngine()
        self.vector_search = VectorSearchEngine()
        self.forecasting = ForecastingEngine()
        self.data_ingestion = DataIngestion()
        
        logger.info("E-Commerce Intelligence Engine initialized")
    
    def setup_database(self) -> bool:
        """Set up the database with tables and sample data"""
        try:
            logger.info("Setting up database...")
            
            # Create tables
            if not self.data_ingestion.create_tables():
                logger.error("Failed to create tables")
                return False
            
            # Load sample data
            if not self.data_ingestion.load_sample_data():
                logger.error("Failed to load sample data")
                return False
            
            logger.info("Database setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            return False
    
    def create_product_embeddings(self) -> bool:
        """Create embeddings for all products"""
        try:
            logger.info("Creating product embeddings...")
            
            success = self.vector_search.create_product_embeddings()
            
            if success:
                logger.info("Product embeddings created successfully")
            else:
                logger.error("Failed to create product embeddings")
            
            return success
            
        except Exception as e:
            logger.error(f"Error creating product embeddings: {e}")
            return False
    
    def demonstrate_marketing_engine(self) -> Dict[str, Any]:
        """Demonstrate the marketing engine capabilities"""
        try:
            logger.info("Demonstrating marketing engine...")
            
            results = {}
            
            # Generate personalized email for a user
            user_id = "USER001"
            email_content = self.marketing_engine.generate_personalized_email(
                user_id, "recommendation"
            )
            results['personalized_email'] = email_content
            
            # Generate product recommendations email
            recommendations_email = self.marketing_engine.generate_product_recommendations_email(user_id)
            results['recommendations_email'] = recommendations_email
            
            # Generate abandoned cart email
            abandoned_cart_email = self.marketing_engine.generate_abandoned_cart_email(user_id)
            results['abandoned_cart_email'] = abandoned_cart_email
            
            logger.info("Marketing engine demonstration completed")
            return results
            
        except Exception as e:
            logger.error(f"Error demonstrating marketing engine: {e}")
            return {}
    
    def demonstrate_vector_search(self) -> Dict[str, Any]:
        """Demonstrate the vector search capabilities"""
        try:
            logger.info("Demonstrating vector search...")
            
            results = {}
            
            # Find similar products
            product_id = "PROD001"
            similar_products = self.vector_search.find_similar_products(product_id, top_k=3)
            results['similar_products'] = similar_products
            
            # Search products by text
            search_text = "wireless headphones with noise cancellation"
            search_results = self.vector_search.search_products_by_text(search_text, top_k=3)
            results['text_search_results'] = search_results
            
            # Get product substitutions
            substitutions = self.vector_search.get_product_substitutions(product_id, "out_of_stock")
            results['product_substitutions'] = substitutions
            
            # Cross-category recommendations
            user_id = "USER001"
            cross_category_recs = self.vector_search.find_cross_category_recommendations(user_id, top_k=3)
            results['cross_category_recommendations'] = cross_category_recs
            
            logger.info("Vector search demonstration completed")
            return results
            
        except Exception as e:
            logger.error(f"Error demonstrating vector search: {e}")
            return {}
    
    def demonstrate_forecasting(self) -> Dict[str, Any]:
        """Demonstrate the forecasting capabilities"""
        try:
            logger.info("Demonstrating forecasting engine...")
            
            results = {}
            
            # Forecast product demand
            product_id = "PROD001"
            demand_forecast = self.forecasting.forecast_product_demand(product_id, forecast_periods=30)
            results['product_demand_forecast'] = demand_forecast
            
            # Forecast category demand
            category = "electronics"
            category_forecast = self.forecasting.forecast_category_demand(category, forecast_periods=30)
            results['category_demand_forecast'] = category_forecast
            
            # Forecast revenue
            revenue_forecast = self.forecasting.forecast_revenue(forecast_periods=30)
            results['revenue_forecast'] = revenue_forecast
            
            # Get inventory forecast
            current_stock = 150
            inventory_forecast = self.forecasting.get_inventory_forecast(product_id, current_stock)
            results['inventory_forecast'] = inventory_forecast
            
            # Get trend analysis
            trend_analysis = self.forecasting.get_trend_analysis(product_id, period_days=30)
            results['trend_analysis'] = trend_analysis
            
            logger.info("Forecasting demonstration completed")
            return results
            
        except Exception as e:
            logger.error(f"Error demonstrating forecasting: {e}")
            return {}
    
    def demonstrate_ai_engine(self) -> Dict[str, Any]:
        """Demonstrate the AI engine capabilities"""
        try:
            logger.info("Demonstrating AI engine...")
            
            results = {}
            
            # Generate text
            prompt = "Create a product description for a wireless Bluetooth speaker"
            generated_text = self.ai_engine.generate_text(prompt)
            results['generated_text'] = generated_text
            
            # Analyze sentiment
            review_text = "This product exceeded my expectations! Great quality and fast delivery."
            sentiment = self.ai_engine.analyze_sentiment(review_text)
            results['sentiment_analysis'] = sentiment
            
            # Summarize text
            long_text = """
            This wireless Bluetooth speaker offers exceptional sound quality with deep bass and clear treble. 
            The battery life is impressive, lasting up to 20 hours on a single charge. The waterproof design 
            makes it perfect for outdoor use. The Bluetooth connectivity is stable and pairs quickly with devices. 
            The build quality is solid and the speaker feels premium. Overall, this is an excellent product 
            that delivers great value for money.
            """
            summary = self.ai_engine.summarize_text(long_text, max_length=100)
            results['text_summary'] = summary
            
            # Extract keywords
            keywords = self.ai_engine.extract_keywords(long_text, max_keywords=5)
            results['extracted_keywords'] = keywords
            
            # Classify text
            categories = ["electronics", "clothing", "home_garden", "sports_outdoors"]
            classification = self.ai_engine.classify_text(long_text, categories)
            results['text_classification'] = classification
            
            logger.info("AI engine demonstration completed")
            return results
            
        except Exception as e:
            logger.error(f"Error demonstrating AI engine: {e}")
            return {}
    
    def run_complete_demo(self) -> Dict[str, Any]:
        """Run a complete demonstration of all components"""
        try:
            logger.info("Starting complete E-Commerce Intelligence demonstration...")
            
            # Setup database
            if not self.setup_database():
                logger.error("Failed to setup database")
                return {}
            
            # Create product embeddings
            if not self.create_product_embeddings():
                logger.warning("Failed to create product embeddings - some features may not work")
            
            # Run all demonstrations
            results = {
                'ai_engine': self.demonstrate_ai_engine(),
                'marketing_engine': self.demonstrate_marketing_engine(),
                'vector_search': self.demonstrate_vector_search(),
                'forecasting': self.demonstrate_forecasting()
            }
            
            logger.info("Complete demonstration finished successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error running complete demo: {e}")
            return {}
    
    def generate_business_insights(self) -> Dict[str, Any]:
        """Generate comprehensive business insights"""
        try:
            logger.info("Generating business insights...")
            
            insights = {
                'summary': 'E-Commerce Intelligence Insights Report',
                'generated_at': str(pd.Timestamp.now()),
                'recommendations': [],
                'metrics': {},
                'forecasts': {}
            }
            
            # Get revenue forecast
            revenue_forecast = self.forecasting.forecast_revenue(forecast_periods=30)
            if revenue_forecast:
                insights['forecasts']['revenue'] = revenue_forecast
            
            # Get top product recommendations
            user_id = "USER001"
            recommendations = self.vector_search.find_cross_category_recommendations(user_id, top_k=5)
            if recommendations:
                insights['recommendations'].append({
                    'type': 'product_recommendations',
                    'data': recommendations
                })
            
            # Generate marketing insights
            marketing_insights = self.marketing_engine.generate_bulk_marketing_campaign("active", "recommendation")
            if marketing_insights:
                insights['recommendations'].append({
                    'type': 'marketing_campaign',
                    'data': marketing_insights
                })
            
            logger.info("Business insights generated successfully")
            return insights
            
        except Exception as e:
            logger.error(f"Error generating business insights: {e}")
            return {}

def main():
    """Main function to run the E-Commerce Intelligence Engine"""
    try:
        # Initialize the engine
        engine = ECommerceIntelligenceEngine()
        
        # Check command line arguments
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command == "setup":
                # Just setup the database
                success = engine.setup_database()
                if success:
                    print("Database setup completed successfully")
                else:
                    print("Database setup failed")
                    sys.exit(1)
            
            elif command == "embeddings":
                # Create embeddings
                success = engine.create_product_embeddings()
                if success:
                    print("Product embeddings created successfully")
                else:
                    print("Failed to create product embeddings")
                    sys.exit(1)
            
            elif command == "demo":
                # Run complete demo
                results = engine.run_complete_demo()
                if results:
                    print("Demo completed successfully")
                    print("Results available in the returned dictionary")
                else:
                    print("Demo failed")
                    sys.exit(1)
            
            elif command == "insights":
                # Generate business insights
                insights = engine.generate_business_insights()
                if insights:
                    print("Business insights generated successfully")
                    print("Insights available in the returned dictionary")
                else:
                    print("Failed to generate business insights")
                    sys.exit(1)
            
            else:
                print(f"Unknown command: {command}")
                print("Available commands: setup, embeddings, demo, insights")
                sys.exit(1)
        
        else:
            # Default: run complete demo
            print("Running complete E-Commerce Intelligence demonstration...")
            results = engine.run_complete_demo()
            
            if results:
                print("\n" + "="*50)
                print("DEMONSTRATION COMPLETED SUCCESSFULLY")
                print("="*50)
                print("\nKey Results:")
                
                # Print summary of results
                for component, component_results in results.items():
                    print(f"\n{component.upper()}:")
                    if component_results:
                        print(f"  - {len(component_results)} operations completed")
                    else:
                        print("  - No results available")
                
                print(f"\nDataset: {config.dataset_ref}")
                print("Check the BigQuery console to view the created tables and data.")
                
            else:
                print("Demonstration failed")
                sys.exit(1)
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
