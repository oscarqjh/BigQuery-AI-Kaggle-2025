"""
Main Application for Smart E-Commerce Intelligence & Recommendation Engine

Demonstrates the complete pipeline with all components working together.
"""

import logging
import sys
import os
import json
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
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

    def _export_demo_results(self, results: Dict[str, Any]) -> Dict[str, str]:
        """Persist demo results: index JSON + per-component JSONs + HTML. Atomic writes."""
        try:
            # 1) Save to files
            base_dir = os.path.join(os.getcwd(), "outputs")
            os.makedirs(base_dir, exist_ok=True)

            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            run_id = f"demo_{timestamp}"
            run_dir = os.path.join(base_dir, run_id)
            os.makedirs(run_dir, exist_ok=True)

            # Helper: truncate large forecast arrays for JSON/HTML readability
            def _truncate_forecasting_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
                if not isinstance(payload, dict):
                    return payload
                def _cap(obj: Any, cap: int = 100) -> Any:
                    if isinstance(obj, list) and len(obj) > cap:
                        return obj[:cap]
                    return obj
                out = {}
                for k, v in payload.items():
                    if isinstance(v, dict):
                        v2 = dict(v)
                        if isinstance(v2.get('predictions'), list):
                            v2['predictions'] = _cap(v2['predictions'])
                            v2['truncated'] = True
                            v2['note'] = 'Predictions truncated for display; see CSV or rerun exporter to write full arrays.'
                        out[k] = v2
                    elif k == 'trend_data' and isinstance(v, list):
                        out[k] = _cap(v)
                    else:
                        out[k] = v
                return out

            # Write per-component JSONs (atomic-ish with Windows fallbacks)
            component_paths: Dict[str, str] = {}
            for component, payload in results.items():
                comp_path = os.path.join(run_dir, f"{component}.json")
                tmp_path = comp_path + ".tmp"
                try:
                    to_write = payload
                    if component == 'forecasting':
                        to_write = _truncate_forecasting_payload(payload)
                    with open(tmp_path, "w", encoding="utf-8") as f:
                        json.dump(to_write, f, ensure_ascii=False, indent=2)
                        f.flush()
                        try:
                            os.fsync(f.fileno())
                        except Exception:
                            pass
                    moved = False
                    try:
                        os.replace(tmp_path, comp_path)
                        moved = True
                    except Exception:
                        try:
                            os.rename(tmp_path, comp_path)
                            moved = True
                        except Exception:
                            try:
                                import shutil
                                shutil.copyfile(tmp_path, comp_path)
                                moved = True
                            except Exception:
                                # As a last resort, keep the tmp file
                                comp_path = tmp_path
                    component_paths[component] = comp_path
                finally:
                    # If both replace and rename worked, tmp should not exist
                    if os.path.exists(tmp_path) and os.path.exists(comp_path) and tmp_path != comp_path:
                        try:
                            os.remove(tmp_path)
                        except Exception:
                            pass

            # Special handling: export large forecasting arrays to CSV for easy viewing
            forecasting_links: Dict[str, str] = {}
            try:
                import csv
                forecasting_payload = results.get('forecasting') or {}
                def _export_predictions(name: str, obj: Dict[str, Any]):
                    preds = (obj or {}).get('predictions')
                    if isinstance(preds, list) and preds:
                        csv_path = os.path.join(run_dir, f"{name}_predictions.csv")
                        with open(csv_path, 'w', newline='', encoding='utf-8') as cf:
                            writer = csv.DictWriter(cf, fieldnames=sorted({k for row in preds for k in row.keys()}))
                            writer.writeheader()
                            for row in preds:
                                writer.writerow(row)
                        forecasting_links[name] = csv_path

                _export_predictions('product_demand_forecast', forecasting_payload.get('product_demand_forecast'))
                _export_predictions('category_demand_forecast', forecasting_payload.get('category_demand_forecast'))
                _export_predictions('revenue_forecast', forecasting_payload.get('revenue_forecast'))
                inv = forecasting_payload.get('inventory_forecast') or {}
                _export_predictions('inventory_demand_forecast', inv.get('demand_forecast'))
            except Exception:
                pass

            # Write index JSON (paths only to avoid massive single file)
            index_path = os.path.join(run_dir, "index.json")
            tmp_index = index_path + ".tmp"
            index_payload = {
                "run_id": run_id,
                "generated_at": timestamp,
                "dataset": config.dataset_ref,
                "components": component_paths,
            }
            with open(tmp_index, "w", encoding="utf-8") as f:
                json.dump(index_payload, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_index, index_path)

            # Simple HTML report
            def _safe(obj: Any) -> str:
                try:
                    return json.dumps(obj, ensure_ascii=False, indent=2)
                except Exception:
                    return str(obj)

            html_sections: List[str] = [
                f"<h1>Smart E-Commerce Intelligence Demo</h1>",
                f"<p><strong>Run ID:</strong> {run_id}</p>",
                f"<p><strong>Generated At (UTC):</strong> {timestamp}</p>",
                f"<p><strong>Dataset:</strong> {config.dataset_ref}</p>",
            ]
            for component, payload in results.items():
                html_sections.append(f"<h2>{component.upper()}</h2>")
                rel_path = component_paths.get(component, "")
                if rel_path:
                    rel_disp = rel_path.replace(os.getcwd() + os.sep, "")
                    html_sections.append(f"<p><a href=\"{rel_disp}\">Open {component}.json</a></p>")
                # Forecasting: link CSVs for full predictions; keep HTML succinct
                if component == 'forecasting' and forecasting_links:
                    html_sections.append("<ul>")
                    for k, v in forecasting_links.items():
                        rel_csv = v.replace(os.getcwd() + os.sep, "")
                        html_sections.append(f"<li><a href=\"{rel_csv}\">{k} predictions (CSV)</a></li>")
                    html_sections.append("</ul>")
                html_sections.append(f"<pre>{_safe(payload)[:1500]}\n... (truncated in HTML; see JSON/CSV links above)</pre>")

            html_doc = """
<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>E-Commerce Intelligence Demo Report</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    h1 { margin-bottom: 4px; }
    h2 { margin-top: 24px; }
    pre { background: #f6f8fa; padding: 12px; overflow-x: auto; border: 1px solid #e1e4e8; border-radius: 6px; }
    p { margin: 4px 0; }
  </style>
  </head>
<body>
%s
</body>
</html>
""" % ("\n".join(html_sections))

            html_path = os.path.join(run_dir, "report.html")
            tmp_html = html_path + ".tmp"
            with open(tmp_html, "w", encoding="utf-8") as f:
                f.write(html_doc)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_html, html_path)

            # 2) Upsert summary rows into BigQuery (optional best-effort)
            try:
                from google.cloud import bigquery
                bq_client = self.data_ingestion.client
                table_id = f"{config.dataset_ref}.demo_results"

                schema = [
                    bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("generated_at", "TIMESTAMP", mode="REQUIRED"),
                    bigquery.SchemaField("component", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("payload", "JSON"),
                ]
                table = bigquery.Table(table_id, schema=schema)
                bq_client.create_table(table, exists_ok=True)

                rows = []
                for component, payload in results.items():
                    rows.append({
                        "run_id": run_id,
                        "generated_at": datetime.utcnow().isoformat(),
                        "component": component,
                        "payload": payload,
                    })
                bq_client.insert_rows_json(table_id, rows)
            except Exception:
                # Non-fatal: continue even if upsert fails
                pass

            exports = {"index_json": index_path, "html_path": html_path, **{f"{k}_json": v for k, v in component_paths.items()}}
            # include forecasting CSVs if any
            for k, v in forecasting_links.items():
                exports[f"{k}_csv"] = v
            return exports
        except Exception:
            return {}
    
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
                # Forecasting results intentionally disabled from export
                'forecasting': {}
            }

            # Export results for user to view
            export_paths = self._export_demo_results(results)
            
            logger.info("Complete demonstration finished successfully")
            if export_paths:
                logger.info(f"Demo results saved: JSON={export_paths.get('json_path')}, HTML={export_paths.get('html_path')}")
                results["exports"] = export_paths
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
                if isinstance(results.get("exports"), dict):
                    print(f"\nLocal report saved to:\n  JSON: {results['exports'].get('json_path')}\n  HTML: {results['exports'].get('html_path')}")
                
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
