"""
AI Engine for Smart E-Commerce Intelligence

Handles text generation, embeddings, and AI operations using BigQuery AI functions.
"""

import logging
from typing import List, Dict, Any, Optional
from google.cloud import bigquery
from config.bigquery_config import get_bigquery_client, config
from config.settings import get_ai_model_config

logger = logging.getLogger(__name__)

class AIEngine:
    """Core AI engine for handling BigQuery AI operations"""
    
    def __init__(self):
        self.client = get_bigquery_client()
        self.text_config = get_ai_model_config('text_generation')
        self.embedding_config = get_ai_model_config('embedding')
    
    def generate_text(self, prompt: str, max_tokens: Optional[int] = None) -> str:
        """
        Generate text using BigQuery AI.GENERATE
        
        Args:
            prompt: The input prompt for text generation
            max_tokens: Maximum tokens to generate (optional)
            
        Returns:
            Generated text
        """
        try:
            max_tokens = max_tokens or self.text_config.get('max_tokens', 1024)
            
            # Escape single quotes in the prompt
            safe_prompt = prompt.replace("'", "''")
            
            query = f"""
            SELECT AI.GENERATE(
                prompt => '{safe_prompt}',
                model_params => JSON '{{"max_tokens": {max_tokens}, "temperature": {self.text_config.get('temperature', 0.7)}, "top_p": {self.text_config.get('top_p', 0.8)}, "top_k": {self.text_config.get('top_k', 40)}}}'
            ) AS generated_text
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return row.generated_text
            
            return ""
            
        except Exception as e:
            logger.error(f"Error generating text: {e}")
            raise
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate embeddings using BigQuery ML.GENERATE_EMBEDDING
        
        Args:
            text: Input text to embed
            
        Returns:
            Embedding vector
        """
        try:
            # Escape single quotes in the text
            safe_text = text.replace("'", "''")
            
            query = f"""
            SELECT ML.GENERATE_EMBEDDING(
                '{safe_text}',
                model => '{self.embedding_config['model']}'
            ) AS embedding
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return row.embedding
            
            return []
            
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            raise
    
    def batch_generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts in batch
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embedding vectors
        """
        try:
            # Create temporary table with texts
            temp_table_id = f"{config.dataset_ref}.temp_texts_{hash(str(texts)) % 10000}"
            
            # Create schema for temporary table
            schema = [
                bigquery.SchemaField("id", "INTEGER"),
                bigquery.SchemaField("text", "STRING")
            ]
            
            table = bigquery.Table(temp_table_id, schema=schema)
            self.client.create_table(table, exists_ok=True)
            
            # Insert texts
            rows_to_insert = [{"id": i, "text": text} for i, text in enumerate(texts)]
            errors = self.client.insert_rows_json(table, rows_to_insert)
            
            if errors:
                raise Exception(f"Error inserting data: {errors}")
            
            # Generate embeddings
            query = f"""
            SELECT 
                id,
                ML.GENERATE_EMBEDDING(
                    text,
                    model => '{self.embedding_config['model']}'
                ) AS embedding
            FROM `{temp_table_id}`
            ORDER BY id
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            embeddings = [None] * len(texts)
            for row in results:
                embeddings[row.id] = row.embedding
            
            # Clean up temporary table
            self.client.delete_table(temp_table_id, not_found_ok=True)
            
            return embeddings
            
        except Exception as e:
            logger.error(f"Error in batch embedding generation: {e}")
            raise
    
    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """
        Analyze sentiment of text using AI
        
        Args:
            text: Text to analyze
            
        Returns:
            Sentiment analysis results
        """
        try:
            prompt = f"""
            Analyze the sentiment of the following text and provide:
            1. Overall sentiment (positive, negative, neutral)
            2. Confidence score (0-1)
            3. Key emotions detected
            4. Summary in JSON format
            
            Text: {text}
            """
            
            result = self.generate_text(prompt)
            
            # Parse the JSON result (assuming the AI returns JSON)
            import json
            try:
                return json.loads(result)
            except:
                return {
                    "sentiment": "neutral",
                    "confidence": 0.5,
                    "emotions": [],
                    "raw_result": result
                }
                
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            raise
    
    def summarize_text(self, text: str, max_length: int = 200) -> str:
        """
        Summarize text using AI
        
        Args:
            text: Text to summarize
            max_length: Maximum length of summary
            
        Returns:
            Summarized text
        """
        try:
            prompt = f"""
            Summarize the following text in {max_length} characters or less:
            
            {text}
            
            Provide a concise summary that captures the key points.
            """
            
            return self.generate_text(prompt)
            
        except Exception as e:
            logger.error(f"Error summarizing text: {e}")
            raise
    
    def extract_keywords(self, text: str, num_keywords: int = 10) -> List[str]:
        """
        Extract keywords from text using AI
        
        Args:
            text: Text to extract keywords from
            num_keywords: Number of keywords to extract
            
        Returns:
            List of keywords
        """
        try:
            prompt = f"""
            Extract the top {num_keywords} most important keywords from the following text.
            Return only the keywords separated by commas:
            
            {text}
            """
            
            result = self.generate_text(prompt)
            keywords = [kw.strip() for kw in result.split(',')]
            return keywords[:num_keywords]
            
        except Exception as e:
            logger.error(f"Error extracting keywords: {e}")
            raise
    
    def classify_text(self, text: str, categories: List[str]) -> Dict[str, float]:
        """
        Classify text into categories using AI
        
        Args:
            text: Text to classify
            categories: List of possible categories
            
        Returns:
            Dictionary of category probabilities
        """
        try:
            categories_str = ', '.join(categories)
            prompt = f"""
            Classify the following text into one of these categories: {categories_str}
            
            Text: {text}
            
            Return the classification as JSON with category names as keys and confidence scores (0-1) as values.
            """
            
            result = self.generate_text(prompt)
            
            import json
            try:
                return json.loads(result)
            except:
                # Fallback: return equal probabilities
                return {cat: 1.0/len(categories) for cat in categories}
                
        except Exception as e:
            logger.error(f"Error classifying text: {e}")
            raise
