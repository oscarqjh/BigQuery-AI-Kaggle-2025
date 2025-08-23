"""
Marketing Engine for Smart E-Commerce Intelligence

Generates personalized marketing content using BigQuery AI functions.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from google.cloud import bigquery
from config.bigquery_config import get_bigquery_client, config
from config.settings import get_marketing_config
from .ai_engine_simple import SimpleAIEngine as AIEngine

logger = logging.getLogger(__name__)

class MarketingEngine:
    """Marketing engine for generating personalized content"""
    
    def __init__(self):
        self.client = get_bigquery_client()
        self.ai_engine = AIEngine()
        self.config = get_marketing_config()
    
    def generate_personalized_email(self, user_id: str, email_type: str = "recommendation") -> str:
        """
        Generate personalized marketing email for a user
        
        Args:
            user_id: User ID to generate email for
            email_type: Type of email (welcome, recommendation, discount, abandoned_cart)
            
        Returns:
            Personalized email content
        """
        try:
            # Get user data
            user_data = self._get_user_data(user_id)
            if not user_data:
                return self._generate_generic_email(email_type)
            
            # Build personalized prompt
            prompt = self._build_email_prompt(user_data, email_type)
            
            # Generate email using AI
            email_content = self.ai_engine.generate_text(prompt)
            
            return email_content
            
        except Exception as e:
            logger.error(f"Error generating personalized email: {e}")
            return self._generate_generic_email(email_type)
    
    def generate_bulk_marketing_campaign(self, user_segment: str, campaign_type: str) -> List[Dict[str, Any]]:
        """
        Generate marketing campaign for a user segment
        
        Args:
            user_segment: Segment of users (new, active, inactive, etc.)
            campaign_type: Type of campaign
            
        Returns:
            List of personalized emails for the segment
        """
        try:
            # Get users in segment
            users = self._get_users_by_segment(user_segment)
            
            campaign_emails = []
            for user in users:
                email_content = self.generate_personalized_email(
                    user['user_id'], 
                    campaign_type
                )
                
                campaign_emails.append({
                    'user_id': user['user_id'],
                    'email': user['email'],
                    'content': email_content,
                    'campaign_type': campaign_type,
                    'generated_at': datetime.now().isoformat()
                })
            
            return campaign_emails
            
        except Exception as e:
            logger.error(f"Error generating bulk campaign: {e}")
            raise
    
    def generate_product_recommendations_email(self, user_id: str) -> str:
        """
        Generate email with product recommendations
        
        Args:
            user_id: User ID
            
        Returns:
            Email with product recommendations
        """
        try:
            # Get user's recent behavior and preferences
            user_preferences = self._get_user_preferences(user_id)
            
            # Get recommended products
            recommended_products = self._get_recommended_products(user_id, limit=5)
            
            if not recommended_products:
                return self._generate_generic_email("recommendation")
            
            # Build recommendation prompt
            prompt = f"""
            Create a personalized email recommending products to a user with the following preferences:
            
            User Preferences: {user_preferences}
            
            Recommended Products:
            {self._format_products_for_prompt(recommended_products)}
            
            Make the email engaging, personal, and include specific reasons why these products would be perfect for this user.
            Keep it under 300 words and include a clear call-to-action.
            """
            
            return self.ai_engine.generate_text(prompt)
            
        except Exception as e:
            logger.error(f"Error generating recommendations email: {e}")
            return self._generate_generic_email("recommendation")
    
    def generate_abandoned_cart_email(self, user_id: str) -> str:
        """
        Generate email for abandoned cart recovery
        
        Args:
            user_id: User ID
            
        Returns:
            Abandoned cart recovery email
        """
        try:
            # Get abandoned cart items
            cart_items = self._get_abandoned_cart_items(user_id)
            
            if not cart_items:
                return self._generate_generic_email("abandoned_cart")
            
            # Build abandoned cart prompt
            prompt = f"""
            Create an email to recover an abandoned cart with the following items:
            
            Cart Items:
            {self._format_cart_items_for_prompt(cart_items)}
            
            Make the email urgent but friendly, highlight the value of the items, and include a special discount if appropriate.
            Keep it under 250 words with a clear call-to-action to complete the purchase.
            """
            
            return self.ai_engine.generate_text(prompt)
            
        except Exception as e:
            logger.error(f"Error generating abandoned cart email: {e}")
            return self._generate_generic_email("abandoned_cart")
    
    def generate_seasonal_campaign(self, season: str, user_segment: str = "all") -> List[Dict[str, Any]]:
        """
        Generate seasonal marketing campaign
        
        Args:
            season: Season (summer, winter, holiday, etc.)
            user_segment: Target user segment
            
        Returns:
            List of seasonal campaign emails
        """
        try:
            # Get seasonal products
            seasonal_products = self._get_seasonal_products(season)
            
            # Get target users
            users = self._get_users_by_segment(user_segment)
            
            campaign_emails = []
            for user in users:
                prompt = f"""
                Create a seasonal {season} marketing email for a user with the following seasonal products:
                
                Seasonal Products:
                {self._format_products_for_prompt(seasonal_products[:3])}
                
                Make it festive and seasonal, highlight the limited-time nature of the offers, and create urgency.
                Keep it under 300 words with clear seasonal messaging.
                """
                
                email_content = self.ai_engine.generate_text(prompt)
                
                campaign_emails.append({
                    'user_id': user['user_id'],
                    'email': user['email'],
                    'content': email_content,
                    'campaign_type': f'seasonal_{season}',
                    'generated_at': datetime.now().isoformat()
                })
            
            return campaign_emails
            
        except Exception as e:
            logger.error(f"Error generating seasonal campaign: {e}")
            raise
    
    def _get_user_data(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user data for personalization"""
        try:
            query = f"""
            SELECT 
                u.user_id,
                u.email,
                u.first_name,
                u.last_name,
                u.demographics,
                u.registration_date,
                COUNT(o.order_id) as total_orders,
                AVG(o.total_amount) as avg_order_value,
                MAX(o.order_date) as last_order_date
            FROM `{config.users_table}` u
            LEFT JOIN `{config.orders_table}` o ON u.user_id = o.user_id
            WHERE u.user_id = '{user_id}'
            GROUP BY u.user_id, u.email, u.first_name, u.last_name, u.demographics, u.registration_date
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return {
                    'user_id': row.user_id,
                    'email': row.email,
                    'first_name': row.first_name,
                    'last_name': row.last_name,
                    'demographics': row.demographics,
                    'registration_date': row.registration_date,
                    'total_orders': row.total_orders,
                    'avg_order_value': row.avg_order_value,
                    'last_order_date': row.last_order_date
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting user data: {e}")
            return None
    
    def _get_user_preferences(self, user_id: str) -> Dict[str, Any]:
        """Get user preferences and behavior patterns"""
        try:
            query = f"""
            SELECT 
                p.category,
                COUNT(*) as view_count,
                AVG(p.rating) as avg_rating
            FROM `{config.user_behavior_table}` ub
            JOIN `{config.products_table}` p ON ub.product_id = p.product_id
            WHERE ub.user_id = '{user_id}'
            GROUP BY p.category
            ORDER BY view_count DESC
            LIMIT 5
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            preferences = {
                'top_categories': [],
                'avg_rating': 0,
                'total_views': 0
            }
            
            for row in results:
                preferences['top_categories'].append({
                    'category': row.category,
                    'view_count': row.view_count,
                    'avg_rating': row.avg_rating
                })
            
            return preferences
            
        except Exception as e:
            logger.error(f"Error getting user preferences: {e}")
            return {'top_categories': [], 'avg_rating': 0, 'total_views': 0}
    
    def _get_recommended_products(self, user_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get recommended products for user"""
        try:
            query = f"""
            SELECT 
                p.product_id,
                p.name,
                p.description,
                p.price,
                p.category,
                p.rating,
                p.image_url
            FROM `{config.products_table}` p
            WHERE p.stock_quantity > 0
            ORDER BY p.rating DESC, p.price ASC
            LIMIT {limit}
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
                    'image_url': row.image_url
                })
            
            return products
            
        except Exception as e:
            logger.error(f"Error getting recommended products: {e}")
            return []
    
    def _get_abandoned_cart_items(self, user_id: str) -> List[Dict[str, Any]]:
        """Get abandoned cart items for user"""
        try:
            query = f"""
            SELECT 
                p.product_id,
                p.name,
                p.price,
                p.image_url,
                c.quantity
            FROM `{config.user_behavior_table}` c
            JOIN `{config.products_table}` p ON c.product_id = p.product_id
            WHERE c.user_id = '{user_id}'
            AND c.action_type = 'add_to_cart'
            AND c.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            AND p.stock_quantity > 0
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            cart_items = []
            for row in results:
                cart_items.append({
                    'product_id': row.product_id,
                    'name': row.name,
                    'price': row.price,
                    'image_url': row.image_url,
                    'quantity': row.quantity
                })
            
            return cart_items
            
        except Exception as e:
            logger.error(f"Error getting abandoned cart items: {e}")
            return []
    
    def _get_users_by_segment(self, segment: str) -> List[Dict[str, Any]]:
        """Get users by segment"""
        try:
            if segment == "all":
                query = f"""
                SELECT user_id, email, first_name, last_name
                FROM `{config.users_table}`
                LIMIT 100
                """
            else:
                query = f"""
                SELECT user_id, email, first_name, last_name
                FROM `{config.users_table}`
                WHERE user_segment = '{segment}'
                LIMIT 100
                """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            users = []
            for row in results:
                users.append({
                    'user_id': row.user_id,
                    'email': row.email,
                    'first_name': row.first_name,
                    'last_name': row.last_name
                })
            
            return users
            
        except Exception as e:
            logger.error(f"Error getting users by segment: {e}")
            return []
    
    def _get_seasonal_products(self, season: str) -> List[Dict[str, Any]]:
        """Get seasonal products"""
        try:
            query = f"""
            SELECT 
                product_id,
                name,
                description,
                price,
                category,
                rating
            FROM `{config.products_table}`
            WHERE LOWER(description) LIKE '%{season}%'
            OR LOWER(category) LIKE '%{season}%'
            AND stock_quantity > 0
            ORDER BY rating DESC
            LIMIT 10
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
                    'rating': row.rating
                })
            
            return products
            
        except Exception as e:
            logger.error(f"Error getting seasonal products: {e}")
            return []
    
    def _build_email_prompt(self, user_data: Dict[str, Any], email_type: str) -> str:
        """Build personalized email prompt"""
        template = self.config['email_templates'].get(email_type, "")
        
        # Escape special characters in the template
        safe_template = template.replace("'", "''").replace('"', '""') if template else ""
        
        prompt = f"""
        Create a personalized marketing email for a user with the following information:
        
        User Information:
        - Name: {user_data.get('first_name', 'Valued Customer')} {user_data.get('last_name', '')}
        - Total Orders: {user_data.get('total_orders', 0)}
        - Average Order Value: ${user_data.get('avg_order_value', 0):.2f}
        - Last Order: {user_data.get('last_order_date', 'Never')}
        
        Email Type: {email_type}
        
        Base Template: {safe_template}
        
        Make the email personal, engaging, and relevant to this specific user's behavior and preferences.
        Include their name and reference their purchase history when appropriate.
        Keep it under 300 words with a clear call-to-action.
        """
        
        return prompt
    
    def _generate_generic_email(self, email_type: str) -> str:
        """Generate generic email when personalization fails"""
        template = self.config['email_templates'].get(email_type, "")
        
        # Escape special characters in the template
        safe_template = template.replace("'", "''").replace('"', '""') if template else ""
        
        prompt = f"""
        Create a generic marketing email using this template: {safe_template}
        
        Make it engaging and professional, but generic enough to work for any customer.
        Keep it under 250 words with a clear call-to-action.
        """
        
        return self.ai_engine.generate_text(prompt)
    
    def _format_products_for_prompt(self, products: List[Dict[str, Any]]) -> str:
        """Format products for AI prompt"""
        formatted = []
        for product in products:
            formatted.append(f"- {product['name']} (${product['price']:.2f}, Rating: {product['rating']:.1f})")
        return "\n".join(formatted)
    
    def _format_cart_items_for_prompt(self, cart_items: List[Dict[str, Any]]) -> str:
        """Format cart items for AI prompt"""
        formatted = []
        for item in cart_items:
            formatted.append(f"- {item['name']} (${item['price']:.2f}, Qty: {item['quantity']})")
        return "\n".join(formatted)
