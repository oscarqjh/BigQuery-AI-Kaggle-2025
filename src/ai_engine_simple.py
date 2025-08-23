"""
Simplified AI Engine for Smart E-Commerce Intelligence

Uses working BigQuery functions for text processing and analysis.
"""

import logging
import json
from typing import List, Dict, Any, Optional
from google.cloud import bigquery
from config.bigquery_config import get_bigquery_client, config

logger = logging.getLogger(__name__)

class SimpleAIEngine:
    """Simplified AI engine using working BigQuery functions"""
    
    def __init__(self):
        self.client = get_bigquery_client()
    
    def generate_text(self, prompt: str, max_tokens: Optional[int] = None) -> str:
        """
        Generate text using a simple template-based approach
        
        Args:
            prompt: The input prompt for text generation
            max_tokens: Maximum tokens to generate (optional)
            
        Returns:
            Generated text
        """
        try:
            # For now, return a template-based response
            # In a real implementation, you would use Vertex AI or other AI services
            
            if "marketing" in prompt.lower():
                return self._generate_marketing_content(prompt)
            elif "review" in prompt.lower():
                return self._generate_review_summary(prompt)
            elif "recommendation" in prompt.lower():
                return self._generate_recommendation(prompt)
            else:
                return self._generate_generic_response(prompt)
            
        except Exception as e:
            logger.error(f"Error generating text: {e}")
            return "I apologize, but I'm unable to generate content at the moment."
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate a simple hash-based embedding (placeholder)
        
        Args:
            text: Input text to embed
            
        Returns:
            Simple embedding vector
        """
        try:
            # Create a simple hash-based embedding for demonstration
            # In production, use proper embedding services
            import hashlib
            
            # Create a 768-dimensional vector based on text hash
            text_hash = hashlib.md5(text.encode()).hexdigest()
            embedding = []
            
            for i in range(768):
                # Use hash to generate pseudo-random values
                hash_part = text_hash[i % len(text_hash)]
                value = (ord(hash_part) - ord('a')) / 26.0  # Normalize to 0-1
                embedding.append(value)
            
            return embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return [0.0] * 768
    
    def batch_generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts in batch
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embedding vectors
        """
        try:
            embeddings = []
            for text in texts:
                embedding = self.generate_embedding(text)
                embeddings.append(embedding)
            
            return embeddings
            
        except Exception as e:
            logger.error(f"Error in batch embedding generation: {e}")
            return [[0.0] * 768] * len(texts)
    
    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """
        Analyze sentiment using simple keyword analysis
        
        Args:
            text: Text to analyze
            
        Returns:
            Sentiment analysis results
        """
        try:
            text_lower = text.lower()
            
            # Simple keyword-based sentiment analysis
            positive_words = ['good', 'great', 'excellent', 'amazing', 'love', 'perfect', 'wonderful', 'fantastic']
            negative_words = ['bad', 'terrible', 'awful', 'hate', 'worst', 'disappointing', 'poor', 'horrible']
            
            positive_count = sum(1 for word in positive_words if word in text_lower)
            negative_count = sum(1 for word in negative_words if word in text_lower)
            
            if positive_count > negative_count:
                sentiment = "positive"
                confidence = min(0.9, 0.5 + (positive_count - negative_count) * 0.1)
            elif negative_count > positive_count:
                sentiment = "negative"
                confidence = min(0.9, 0.5 + (negative_count - positive_count) * 0.1)
            else:
                sentiment = "neutral"
                confidence = 0.5
            
            return {
                "sentiment": sentiment,
                "confidence": confidence,
                "positive_score": positive_count,
                "negative_score": negative_count,
                "text_length": len(text)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {e}")
            return {
                "sentiment": "neutral",
                "confidence": 0.5,
                "positive_score": 0,
                "negative_score": 0,
                "text_length": len(text)
            }
    
    def summarize_text(self, text: str, max_length: int = 200) -> str:
        """
        Summarize text using simple extraction
        
        Args:
            text: Text to summarize
            max_length: Maximum length of summary
            
        Returns:
            Summarized text
        """
        try:
            # Simple extractive summarization
            sentences = text.split('.')
            if len(sentences) <= 2:
                return text[:max_length]
            
            # Take first and last sentences
            summary = sentences[0] + '. ' + sentences[-1] + '.'
            
            if len(summary) > max_length:
                summary = summary[:max_length-3] + '...'
            
            return summary
            
        except Exception as e:
            logger.error(f"Error summarizing text: {e}")
            return text[:max_length] if len(text) > max_length else text
    
    def extract_keywords(self, text: str, max_keywords: int = 10) -> List[str]:
        """
        Extract keywords using simple frequency analysis
        
        Args:
            text: Text to extract keywords from
            max_keywords: Maximum number of keywords
            
        Returns:
            List of keywords
        """
        try:
            import re
            from collections import Counter
            
            # Remove punctuation and convert to lowercase
            words = re.findall(r'\b\w+\b', text.lower())
            
            # Remove common stop words
            stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them'}
            
            filtered_words = [word for word in words if word not in stop_words and len(word) > 2]
            
            # Count word frequencies
            word_counts = Counter(filtered_words)
            
            # Return top keywords
            keywords = [word for word, count in word_counts.most_common(max_keywords)]
            
            return keywords
            
        except Exception as e:
            logger.error(f"Error extracting keywords: {e}")
            return []
    
    def classify_text(self, text: str, categories: List[str]) -> Dict[str, float]:
        """
        Classify text into categories using keyword matching
        
        Args:
            text: Text to classify
            categories: List of possible categories
            
        Returns:
            Dictionary of category probabilities
        """
        try:
            text_lower = text.lower()
            scores = {}
            
            for category in categories:
                # Simple keyword matching for each category
                category_keywords = {
                    'electronics': ['electronic', 'device', 'tech', 'computer', 'phone', 'laptop'],
                    'clothing': ['clothing', 'shirt', 'dress', 'pants', 'fashion', 'wear'],
                    'home_garden': ['home', 'garden', 'kitchen', 'furniture', 'decor'],
                    'sports_outdoors': ['sport', 'outdoor', 'fitness', 'exercise', 'athletic']
                }
                
                keywords = category_keywords.get(category.lower(), [category.lower()])
                score = sum(1 for keyword in keywords if keyword in text_lower)
                scores[category] = score / len(keywords) if keywords else 0
            
            # Normalize scores
            total_score = sum(scores.values()) or 1
            normalized_scores = {cat: score / total_score for cat, score in scores.items()}
            
            return normalized_scores
            
        except Exception as e:
            logger.error(f"Error classifying text: {e}")
            return {category: 1.0 / len(categories) for category in categories}
    
    def _generate_marketing_content(self, prompt: str) -> str:
        """Generate marketing content"""
        return """
        Dear Valued Customer,

        We're excited to share some amazing products that we think you'll love! 
        Based on your preferences, we've curated a special selection just for you.

        Don't miss out on these incredible deals - shop now and enjoy exclusive discounts!

        Best regards,
        Your E-Commerce Team
        """
    
    def _generate_review_summary(self, prompt: str) -> str:
        """Generate review summary"""
        return """
        Customer Review Summary:
        
        Overall Rating: 4.5/5
        Key Points:
        - Excellent product quality
        - Great value for money
        - Fast delivery
        - Highly recommended
        
        Areas for improvement: None identified
        """
    
    def _generate_recommendation(self, prompt: str) -> str:
        """Generate product recommendation"""
        return """
        Personalized Recommendations:
        
        Based on your browsing history and preferences, we recommend:
        1. Wireless Bluetooth Headphones - Perfect for your music needs
        2. Smart Fitness Watch - Great for tracking your workouts
        3. Organic Cotton T-Shirt - Comfortable and eco-friendly
        
        These products match your style and previous purchases!
        """
    
    def _generate_generic_response(self, prompt: str) -> str:
        """Generate generic response"""
        return f"""
        Thank you for your inquiry about: {prompt[:100]}...
        
        We're here to help you find exactly what you're looking for. 
        Please let us know if you need any additional assistance!
        """
