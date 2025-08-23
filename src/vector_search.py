"""
Vector Search Engine for Smart E-Commerce Intelligence

Handles semantic product similarity search using BigQuery Vector Search.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from google.cloud import bigquery
from config.bigquery_config import get_bigquery_client, config
from config.settings import get_recommendation_config
from .ai_engine_simple import SimpleAIEngine as AIEngine

logger = logging.getLogger(__name__)

class VectorSearchEngine:
    """Vector search engine for semantic product similarity"""
    
    def __init__(self):
        self.client = get_bigquery_client()
        self.ai_engine = AIEngine()
        self.config = get_recommendation_config()
    
    def create_product_embeddings(self) -> bool:
        """
        Create embeddings for all products and store them in BigQuery
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if embeddings table already has data
            if self._embeddings_table_has_data():
                logger.info("Product embeddings table already has data, skipping embedding creation")
                return True
            
            # First, get all product descriptions
            query = f"""
            SELECT 
                product_id,
                name,
                description,
                category,
                brand
            FROM `{config.products_table}`
            WHERE description IS NOT NULL
            """
            
            query_job = self.client.query(query)
            products = query_job.result()
            
            # Create embeddings table if it doesn't exist
            self._create_embeddings_table()
            
            # Process products in batches
            batch_size = 100
            product_batch = []
            
            for product in products:
                # Combine product information for embedding
                text_for_embedding = f"{product.name} {product.description} {product.category} {product.brand}"
                
                product_batch.append({
                    'product_id': product.product_id,
                    'text_for_embedding': text_for_embedding,
                    'name': product.name,
                    'description': product.description,
                    'category': product.category,
                    'brand': product.brand
                })
                
                if len(product_batch) >= batch_size:
                    self._process_embedding_batch(product_batch)
                    product_batch = []
            
            # Process remaining products
            if product_batch:
                self._process_embedding_batch(product_batch)
            
            # Create vector index (only if it doesn't exist)
            self._create_vector_index()
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating product embeddings: {e}")
            return False
    
    def find_similar_products(self, product_id: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Find similar products using vector search
        
        Args:
            product_id: ID of the product to find similar products for
            top_k: Number of similar products to return
            
        Returns:
            List of similar products with similarity scores
        """
        try:
            # Get the embedding for the target product
            target_embedding = self._get_product_embedding(product_id)
            if not target_embedding:
                return []
            
            # Use simple cosine similarity calculation instead of VECTOR_SEARCH
            query = f"""
            SELECT 
                p.product_id,
                p.name,
                p.description,
                p.price,
                p.category,
                p.rating,
                p.image_url,
                p.stock_quantity,
                -- Calculate cosine similarity manually
                (SELECT 
                    SUM(e1.embedding[i] * e2.embedding[i]) / 
                    (SQRT(SUM(e1.embedding[i] * e1.embedding[i])) * SQRT(SUM(e2.embedding[i] * e2.embedding[i])))
                FROM UNNEST(e1.embedding) AS e1_vals WITH OFFSET i
                JOIN UNNEST(e2.embedding) AS e2_vals WITH OFFSET i USING (i)
                ) AS similarity_score
            FROM `{config.dataset_ref}.product_embeddings` e1
            JOIN `{config.dataset_ref}.product_embeddings` e2 ON e1.product_id = '{product_id}'
            JOIN `{config.products_table}` p ON e1.product_id = p.product_id
            WHERE e1.product_id != '{product_id}'
            AND p.stock_quantity > 0
            ORDER BY similarity_score DESC
            LIMIT {top_k}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            similar_products = []
            for row in results:
                similar_products.append({
                    'product_id': row.product_id,
                    'name': row.name,
                    'description': row.description,
                    'price': row.price,
                    'category': row.category,
                    'rating': row.rating,
                    'image_url': row.image_url,
                    'stock_quantity': row.stock_quantity,
                    'similarity_score': row.similarity_score or 0.5  # Default similarity if calculation fails
                })
            
            return similar_products
            
        except Exception as e:
            logger.error(f"Error finding similar products: {e}")
            # Return fallback recommendations based on category
            return self._get_fallback_recommendations(product_id, top_k)
    
    def search_products_by_text(self, search_text: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Search products by text using semantic similarity
        
        Args:
            search_text: Text to search for
            top_k: Number of results to return
            
        Returns:
            List of relevant products
        """
        try:
            # Generate embedding for search text
            search_embedding = self.ai_engine.generate_embedding(search_text)
            if not search_embedding:
                return self._get_text_search_fallback(search_text, top_k)
            
            # Use simple text search with keyword matching as fallback
            query = f"""
            SELECT 
                p.product_id,
                p.name,
                p.description,
                p.price,
                p.category,
                p.rating,
                p.image_url,
                p.stock_quantity,
                0.8 as relevance_score
            FROM `{config.products_table}` p
            WHERE p.stock_quantity > 0
            AND (
                LOWER(p.name) LIKE LOWER('%{search_text}%')
                OR LOWER(p.description) LIKE LOWER('%{search_text}%')
                OR LOWER(p.category) LIKE LOWER('%{search_text}%')
                OR LOWER(p.brand) LIKE LOWER('%{search_text}%')
            )
            ORDER BY p.rating DESC, p.price ASC
            LIMIT {top_k}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            products = []
            for row in results:
                products.append({
                    'product_id': row.product_id,
                    'name': row.name,
                    'description': row.description,
                    'price': row.price,
                    'category': row.category,
                    'rating': row.rating,
                    'image_url': row.image_url,
                    'stock_quantity': row.stock_quantity,
                    'relevance_score': row.relevance_score
                })
            
            return products
            
        except Exception as e:
            logger.error(f"Error searching products by text: {e}")
            return self._get_text_search_fallback(search_text, top_k)
    
    def get_product_substitutions(self, product_id: str, reason: str = "out_of_stock") -> List[Dict[str, Any]]:
        """
        Get product substitutions for out-of-stock or similar products
        
        Args:
            product_id: ID of the product to find substitutions for
            reason: Reason for substitution (out_of_stock, price, etc.)
            
        Returns:
            List of substitution products
        """
        try:
            # Get similar products
            similar_products = self.find_similar_products(product_id, top_k=10)
            
            # Filter based on reason
            if reason == "out_of_stock":
                # Prefer products with good stock levels
                substitutions = [
                    p for p in similar_products 
                    if p['stock_quantity'] > 10 and p['similarity_score'] > 0.7
                ]
            elif reason == "price":
                # Prefer products with similar or lower price
                original_price = self._get_product_price(product_id)
                if original_price:
                    substitutions = [
                        p for p in similar_products 
                        if p['price'] <= original_price * 1.2 and p['similarity_score'] > 0.6
                    ]
                else:
                    substitutions = similar_products
            else:
                substitutions = similar_products
            
            # Add substitution reason and explanation
            for sub in substitutions:
                sub['substitution_reason'] = reason
                sub['explanation'] = self._generate_substitution_explanation(
                    product_id, sub['product_id'], reason
                )
            
            return substitutions[:5]  # Return top 5 substitutions
            
        except Exception as e:
            logger.error(f"Error getting product substitutions: {e}")
            return []
    
    def find_cross_category_recommendations(self, user_id: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Find cross-category product recommendations based on user behavior
        
        Args:
            user_id: User ID
            top_k: Number of recommendations to return
            
        Returns:
            List of cross-category recommendations
        """
        try:
            # Get user's preferred products
            user_products = self._get_user_preferred_products(user_id)
            if not user_products:
                return self._get_popular_products_recommendations(top_k)
            
            # Get user's preferred categories
            user_categories = list(set([p['category'] for p in user_products]))
            
            # Find products from different categories
            query = f"""
            SELECT 
                p.product_id,
                p.name,
                p.description,
                p.price,
                p.category,
                p.rating,
                p.image_url,
                p.stock_quantity,
                0.7 as similarity_score
            FROM `{config.products_table}` p
            WHERE p.stock_quantity > 0
            AND p.category NOT IN ({','.join([f"'{cat}'" for cat in user_categories])})
            AND p.product_id NOT IN ({','.join([f"'{p['product_id']}'" for p in user_products])})
            ORDER BY p.rating DESC, p.price ASC
            LIMIT {top_k}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            recommendations = []
            for row in results:
                recommendations.append({
                    'product_id': row.product_id,
                    'name': row.name,
                    'description': row.description,
                    'price': row.price,
                    'category': row.category,
                    'rating': row.rating,
                    'image_url': row.image_url,
                    'stock_quantity': row.stock_quantity,
                    'similarity_score': row.similarity_score,
                    'recommendation_type': 'cross_category'
                })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error finding cross-category recommendations: {e}")
            return self._get_popular_products_recommendations(top_k)
    
    def _create_embeddings_table(self):
        """Create the product embeddings table"""
        try:
            schema = [
                bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("brand", "STRING"),
                bigquery.SchemaField("created_at", "TIMESTAMP")
            ]
            
            table_id = f"{config.dataset_ref}.product_embeddings"
            table = bigquery.Table(table_id, schema=schema)
            
            self.client.create_table(table, exists_ok=True)
            logger.info(f"Created embeddings table: {table_id}")
            
        except Exception as e:
            logger.error(f"Error creating embeddings table: {e}")
            raise
    
    def _process_embedding_batch(self, product_batch: List[Dict[str, Any]]):
        """Process a batch of products to generate embeddings"""
        try:
            # Generate embeddings for the batch
            texts = [p['text_for_embedding'] for p in product_batch]
            embeddings = self.ai_engine.batch_generate_embeddings(texts)
            
            # Prepare rows for insertion
            rows_to_insert = []
            for i, product in enumerate(product_batch):
                if embeddings[i]:
                    rows_to_insert.append({
                        'product_id': product['product_id'],
                        'embedding': embeddings[i],
                        'name': product['name'],
                        'description': product['description'],
                        'category': product['category'],
                        'brand': product['brand'],
                        'created_at': datetime.now().isoformat()
                    })
            
            # Insert into embeddings table
            table_id = f"{config.dataset_ref}.product_embeddings"
            errors = self.client.insert_rows_json(table_id, rows_to_insert)
            
            if errors:
                logger.error(f"Error inserting embeddings: {errors}")
            else:
                logger.info(f"Successfully processed {len(rows_to_insert)} product embeddings")
                
        except Exception as e:
            logger.error(f"Error processing embedding batch: {e}")
            raise
    
    def _create_vector_index(self):
        """Create vector index for fast similarity search"""
        try:
            # Check if index already exists
            if self._vector_index_exists():
                logger.info("Vector index already exists, skipping creation")
                return
            
            query = f"""
            CREATE VECTOR INDEX product_embeddings_index
            ON `{config.dataset_ref}.product_embeddings`(embedding)
            OPTIONS (
                index_type = 'IVF',
                distance_type = 'COSINE'
            )
            """
            
            query_job = self.client.query(query)
            query_job.result()
            
            logger.info("Created vector index for product embeddings")
            
        except Exception as e:
            logger.error(f"Error creating vector index: {e}")
            # Don't raise the exception, just log it as the index might already exist
            logger.info("Vector index creation failed, continuing without index")
    
    def _vector_index_exists(self) -> bool:
        """Check if the vector index already exists"""
        try:
            query = f"""
            SELECT COUNT(*) as count
            FROM `{config.dataset_ref}.INFORMATION_SCHEMA.VECTOR_INDEXES`
            WHERE table_name = 'product_embeddings'
            AND index_name = 'product_embeddings_index'
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return row.count > 0
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if vector index exists: {e}")
            return False
    
    def _get_product_embedding(self, product_id: str) -> Optional[List[float]]:
        """Get embedding for a specific product"""
        try:
            query = f"""
            SELECT embedding
            FROM `{config.dataset_ref}.product_embeddings`
            WHERE product_id = '{product_id}'
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return row.embedding
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting product embedding: {e}")
            return None
    
    def _get_product_price(self, product_id: str) -> Optional[float]:
        """Get price for a specific product"""
        try:
            query = f"""
            SELECT price
            FROM `{config.products_table}`
            WHERE product_id = '{product_id}'
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return row.price
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting product price: {e}")
            return None
    
    def _get_user_preferred_products(self, user_id: str) -> List[Dict[str, Any]]:
        """Get user's preferred products based on behavior"""
        try:
            query = f"""
            SELECT 
                p.product_id,
                p.name,
                p.category,
                COUNT(*) as interaction_count
            FROM `{config.user_behavior_table}` ub
            JOIN `{config.products_table}` p ON ub.product_id = p.product_id
            WHERE ub.user_id = '{user_id}'
            AND ub.action_type IN ('view', 'add_to_cart', 'purchase')
            GROUP BY p.product_id, p.name, p.category
            ORDER BY interaction_count DESC
            LIMIT 10
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            products = []
            for row in results:
                products.append({
                    'product_id': row.product_id,
                    'name': row.name,
                    'category': row.category,
                    'interaction_count': row.interaction_count
                })
            
            return products
            
        except Exception as e:
            logger.error(f"Error getting user preferred products: {e}")
            return []
    
    def _calculate_average_embedding(self, embeddings: List[List[float]]) -> str:
        """Calculate average embedding (simplified approach)"""
        if not embeddings:
            return "[]"
        
        # Simple average of embeddings
        avg_embedding = []
        for i in range(len(embeddings[0])):
            avg_val = sum(emb[i] for emb in embeddings) / len(embeddings)
            avg_embedding.append(avg_val)
        
        return str(avg_embedding)
    
    def _generate_substitution_explanation(self, original_id: str, substitution_id: str, reason: str) -> str:
        """Generate explanation for product substitution"""
        try:
            # Get product details
            original_product = self._get_product_details(original_id)
            substitution_product = self._get_product_details(substitution_id)
            
            if not original_product or not substitution_product:
                return "Similar product recommendation"
            
            if reason == "out_of_stock":
                return f"Great alternative to {original_product['name']} - similar features and quality"
            elif reason == "price":
                price_diff = substitution_product['price'] - original_product['price']
                if price_diff < 0:
                    return f"More affordable option: {substitution_product['name']} (${abs(price_diff):.2f} less)"
                else:
                    return f"Premium alternative: {substitution_product['name']} with enhanced features"
            else:
                return f"Similar to {original_product['name']} with comparable quality and features"
                
        except Exception as e:
            logger.error(f"Error generating substitution explanation: {e}")
            return "Similar product recommendation"
    
    def _get_product_details(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Get basic product details"""
        try:
            query = f"""
            SELECT product_id, name, price, category
            FROM `{config.products_table}`
            WHERE product_id = '{product_id}'
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return {
                    'product_id': row.product_id,
                    'name': row.name,
                    'price': row.price,
                    'category': row.category
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting product details: {e}")
            return None
    
    def _get_fallback_recommendations(self, product_id: str, top_k: int) -> List[Dict[str, Any]]:
        """Get fallback recommendations based on category when vector search fails"""
        try:
            # Get the category of the target product
            product_details = self._get_product_details(product_id)
            if not product_details:
                return self._get_popular_products_recommendations(top_k)
            
            # Find products in the same category
            query = f"""
            SELECT 
                p.product_id,
                p.name,
                p.description,
                p.price,
                p.category,
                p.rating,
                p.image_url,
                p.stock_quantity
            FROM `{config.products_table}` p
            WHERE p.category = '{product_details['category']}'
            AND p.product_id != '{product_id}'
            AND p.stock_quantity > 0
            ORDER BY p.rating DESC, p.price ASC
            LIMIT {top_k}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            recommendations = []
            for row in results:
                recommendations.append({
                    'product_id': row.product_id,
                    'name': row.name,
                    'description': row.description,
                    'price': row.price,
                    'category': row.category,
                    'rating': row.rating,
                    'image_url': row.image_url,
                    'stock_quantity': row.stock_quantity,
                    'similarity_score': 0.6  # Default similarity for fallback
                })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error getting fallback recommendations: {e}")
            return self._get_popular_products_recommendations(top_k)
    
    def _get_text_search_fallback(self, search_text: str, top_k: int) -> List[Dict[str, Any]]:
        """Get fallback search results when vector search fails"""
        try:
            # Simple keyword search
            query = f"""
            SELECT 
                p.product_id,
                p.name,
                p.description,
                p.price,
                p.category,
                p.rating,
                p.image_url,
                p.stock_quantity
            FROM `{config.products_table}` p
            WHERE p.stock_quantity > 0
            AND (
                LOWER(p.name) LIKE LOWER('%{search_text}%')
                OR LOWER(p.description) LIKE LOWER('%{search_text}%')
                OR LOWER(p.category) LIKE LOWER('%{search_text}%')
            )
            ORDER BY p.rating DESC, p.price ASC
            LIMIT {top_k}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            products = []
            for row in results:
                products.append({
                    'product_id': row.product_id,
                    'name': row.name,
                    'description': row.description,
                    'price': row.price,
                    'category': row.category,
                    'rating': row.rating,
                    'image_url': row.image_url,
                    'stock_quantity': row.stock_quantity,
                    'relevance_score': 0.5  # Default relevance for fallback
                })
            
            return products
            
        except Exception as e:
            logger.error(f"Error getting text search fallback: {e}")
            return self._get_popular_products_recommendations(top_k)
    
    def _get_popular_products_recommendations(self, top_k: int) -> List[Dict[str, Any]]:
        """Get popular products as a general fallback"""
        try:
            query = f"""
            SELECT 
                p.product_id,
                p.name,
                p.description,
                p.price,
                p.category,
                p.rating,
                p.image_url,
                p.stock_quantity
            FROM `{config.products_table}` p
            WHERE p.stock_quantity > 0
            ORDER BY p.rating DESC, p.price ASC
            LIMIT {top_k}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            products = []
            for row in results:
                products.append({
                    'product_id': row.product_id,
                    'name': row.name,
                    'description': row.description,
                    'price': row.price,
                    'category': row.category,
                    'rating': row.rating,
                    'image_url': row.image_url,
                    'stock_quantity': row.stock_quantity,
                    'similarity_score': 0.5,  # Default similarity for popular products
                    'recommendation_type': 'popular'
                })
            
            return products
            
        except Exception as e:
            logger.error(f"Error getting popular products: {e}")
            return []
    
    def _embeddings_table_has_data(self) -> bool:
        """Check if the embeddings table already has data"""
        try:
            table_id = f"{config.dataset_ref}.product_embeddings"
            query = f"SELECT COUNT(*) as count FROM `{table_id}` LIMIT 1"
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return row.count > 0
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if embeddings table has data: {e}")
            return False
