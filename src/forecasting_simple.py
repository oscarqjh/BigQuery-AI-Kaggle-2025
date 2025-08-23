"""
Simplified Forecasting Engine for Smart E-Commerce Intelligence

Uses statistical methods for demand prediction and time series analysis.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import statistics
from google.cloud import bigquery
from config.bigquery_config import get_bigquery_client, config

logger = logging.getLogger(__name__)

class SimpleForecastingEngine:
    """Simplified forecasting engine using statistical methods"""
    
    def __init__(self):
        self.client = get_bigquery_client()
    
    def forecast_product_demand(self, product_id: str, forecast_periods: int = 30) -> Dict[str, Any]:
        """
        Forecast demand for a specific product using statistical methods
        
        Args:
            product_id: Product ID to forecast
            forecast_periods: Number of periods to forecast
            
        Returns:
            Forecast results with predictions and confidence intervals
        """
        try:
            # Get historical sales data
            query = f"""
            SELECT 
                DATE(order_date) as date,
                SUM(quantity) as sales_quantity
            FROM `{config.orders_table}` o
            JOIN `{config.dataset_ref}.order_items` oi ON o.order_id = oi.order_id
            WHERE oi.product_id = '{product_id}'
            AND DATE(o.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
            GROUP BY DATE(order_date)
            ORDER BY date
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            sales_data = []
            for row in results:
                sales_data.append({
                    'date': row.date,
                    'quantity': row.sales_quantity
                })
            
            if not sales_data:
                return self._generate_default_forecast(product_id, forecast_periods)
            
            # Calculate simple moving average forecast
            predictions = self._calculate_moving_average_forecast(sales_data, forecast_periods)
            
            return {
                'product_id': product_id,
                'forecast_periods': forecast_periods,
                'predictions': predictions,
                'method': 'moving_average',
                'confidence_level': 0.8,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error forecasting product demand: {e}")
            return self._generate_default_forecast(product_id, forecast_periods)
    
    def forecast_category_demand(self, category: str, forecast_periods: int = 30) -> Dict[str, Any]:
        """
        Forecast demand for an entire product category
        
        Args:
            category: Product category to forecast
            forecast_periods: Number of periods to forecast
            
        Returns:
            Category forecast results
        """
        try:
            query = f"""
            SELECT 
                DATE(o.order_date) as date,
                SUM(oi.quantity * oi.price) as total_sales
            FROM `{config.orders_table}` o
            JOIN `{config.dataset_ref}.order_items` oi ON o.order_id = oi.order_id
            JOIN `{config.products_table}` p ON oi.product_id = p.product_id
            WHERE p.category = '{category}'
            AND DATE(o.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
            GROUP BY DATE(o.order_date)
            ORDER BY date
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            sales_data = []
            for row in results:
                sales_data.append({
                    'date': row.date,
                    'sales': row.total_sales
                })
            
            if not sales_data:
                return self._generate_default_category_forecast(category, forecast_periods)
            
            # Calculate trend-based forecast
            predictions = self._calculate_trend_forecast(sales_data, forecast_periods)
            
            return {
                'category': category,
                'forecast_periods': forecast_periods,
                'predictions': predictions,
                'method': 'trend_analysis',
                'confidence_level': 0.75,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error forecasting category demand: {e}")
            return self._generate_default_category_forecast(category, forecast_periods)
    
    def forecast_revenue(self, forecast_periods: int = 30) -> Dict[str, Any]:
        """
        Forecast overall revenue for the business
        
        Args:
            forecast_periods: Number of periods to forecast
            
        Returns:
            Revenue forecast results
        """
        try:
            query = f"""
            SELECT 
                DATE(order_date) as date,
                SUM(total_amount) as daily_revenue
            FROM `{config.orders_table}`
            WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
            GROUP BY DATE(order_date)
            ORDER BY date
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            revenue_data = []
            for row in results:
                revenue_data.append({
                    'date': row.date,
                    'revenue': row.daily_revenue
                })
            
            if not revenue_data:
                return self._generate_default_revenue_forecast(forecast_periods)
            
            # Calculate seasonal forecast
            predictions = self._calculate_seasonal_forecast(revenue_data, forecast_periods)
            
            return {
                'forecast_type': 'revenue',
                'forecast_periods': forecast_periods,
                'predictions': predictions,
                'method': 'seasonal_analysis',
                'confidence_level': 0.7,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error forecasting revenue: {e}")
            return self._generate_default_revenue_forecast(forecast_periods)
    
    def get_trend_analysis(self, product_id: str, period_days: int = 30) -> Dict[str, Any]:
        """
        Analyze demand trends for a product
        
        Args:
            product_id: Product ID
            period_days: Number of days to analyze
            
        Returns:
            Trend analysis results
        """
        try:
            query = f"""
            SELECT 
                DATE(order_date) as date,
                SUM(oi.quantity) as daily_sales
            FROM `{config.orders_table}` o
            JOIN `{config.dataset_ref}.order_items` oi ON o.order_id = oi.order_id
            WHERE oi.product_id = '{product_id}'
            AND DATE(o.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {period_days} DAY)
            GROUP BY DATE(order_date)
            ORDER BY date
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            trend_data = []
            for row in results:
                trend_data.append({
                    'date': row.date,
                    'daily_sales': row.daily_sales
                })
            
            if len(trend_data) < 2:
                return {
                    'product_id': product_id,
                    'trend_direction': 'stable',
                    'trend_strength': 0,
                    'recent_average_daily_sales': 0,
                    'trend_data': trend_data
                }
            
            # Calculate trend indicators
            recent_avg = statistics.mean([d['daily_sales'] for d in trend_data[-7:]])
            older_avg = statistics.mean([d['daily_sales'] for d in trend_data[-14:-7]]) if len(trend_data) >= 14 else recent_avg
            
            trend_direction = "increasing" if recent_avg > older_avg else "decreasing" if recent_avg < older_avg else "stable"
            trend_strength = abs(recent_avg - older_avg) / max(older_avg, 1)
            
            return {
                'product_id': product_id,
                'trend_direction': trend_direction,
                'trend_strength': trend_strength,
                'recent_average_daily_sales': recent_avg,
                'trend_data': trend_data
            }
            
        except Exception as e:
            logger.error(f"Error getting trend analysis: {e}")
            return {
                'product_id': product_id,
                'trend_direction': 'stable',
                'trend_strength': 0,
                'recent_average_daily_sales': 0,
                'trend_data': []
            }
    
    def _calculate_moving_average_forecast(self, sales_data: List[Dict], periods: int) -> List[Dict]:
        """Calculate moving average forecast"""
        if len(sales_data) < 7:
            avg_sales = statistics.mean([d['quantity'] for d in sales_data]) if sales_data else 1
        else:
            # Use 7-day moving average
            recent_sales = [d['quantity'] for d in sales_data[-7:]]
            avg_sales = statistics.mean(recent_sales)
        
        predictions = []
        for i in range(periods):
            predictions.append({
                'period': i + 1,
                'value': avg_sales,
                'confidence': 0.8,
                'date': (datetime.now() + timedelta(days=i+1)).isoformat()
            })
        
        return predictions
    
    def _calculate_trend_forecast(self, sales_data: List[Dict], periods: int) -> List[Dict]:
        """Calculate trend-based forecast"""
        if len(sales_data) < 2:
            avg_sales = statistics.mean([d['sales'] for d in sales_data]) if sales_data else 100
            trend = 0
        else:
            # Calculate trend
            recent_sales = [d['sales'] for d in sales_data[-7:]]
            older_sales = [d['sales'] for d in sales_data[-14:-7]] if len(sales_data) >= 14 else recent_sales
            
            recent_avg = statistics.mean(recent_sales)
            older_avg = statistics.mean(older_sales)
            trend = (recent_avg - older_avg) / max(older_avg, 1)
            avg_sales = recent_avg
        
        predictions = []
        for i in range(periods):
            predicted_value = avg_sales * (1 + trend * (i + 1))
            predictions.append({
                'period': i + 1,
                'value': max(0, predicted_value),
                'confidence': 0.75,
                'date': (datetime.now() + timedelta(days=i+1)).isoformat()
            })
        
        return predictions
    
    def _calculate_seasonal_forecast(self, revenue_data: List[Dict], periods: int) -> List[Dict]:
        """Calculate seasonal forecast"""
        if len(revenue_data) < 7:
            avg_revenue = statistics.mean([d['revenue'] for d in revenue_data]) if revenue_data else 1000
        else:
            # Use weekly pattern
            recent_revenue = [d['revenue'] for d in revenue_data[-7:]]
            avg_revenue = statistics.mean(recent_revenue)
        
        predictions = []
        for i in range(periods):
            # Add some seasonal variation
            day_of_week = (datetime.now() + timedelta(days=i+1)).weekday()
            seasonal_factor = 1.2 if day_of_week in [4, 5] else 0.9 if day_of_week == 0 else 1.0  # Weekend boost
            
            predicted_value = avg_revenue * seasonal_factor
            predictions.append({
                'period': i + 1,
                'value': max(0, predicted_value),
                'confidence': 0.7,
                'date': (datetime.now() + timedelta(days=i+1)).isoformat()
            })
        
        return predictions
    
    def _generate_default_forecast(self, product_id: str, periods: int) -> Dict[str, Any]:
        """Generate default forecast when no data is available"""
        predictions = []
        for i in range(periods):
            predictions.append({
                'period': i + 1,
                'value': 1.0,  # Default 1 unit per day
                'confidence': 0.5,
                'date': (datetime.now() + timedelta(days=i+1)).isoformat()
            })
        
        return {
            'product_id': product_id,
            'forecast_periods': periods,
            'predictions': predictions,
            'method': 'default',
            'confidence_level': 0.5,
            'last_updated': datetime.now().isoformat()
        }
    
    def _generate_default_category_forecast(self, category: str, periods: int) -> Dict[str, Any]:
        """Generate default category forecast"""
        predictions = []
        for i in range(periods):
            predictions.append({
                'period': i + 1,
                'value': 100.0,  # Default $100 per day
                'confidence': 0.5,
                'date': (datetime.now() + timedelta(days=i+1)).isoformat()
            })
        
        return {
            'category': category,
            'forecast_periods': periods,
            'predictions': predictions,
            'method': 'default',
            'confidence_level': 0.5,
            'last_updated': datetime.now().isoformat()
        }
    
    def _generate_default_revenue_forecast(self, periods: int) -> Dict[str, Any]:
        """Generate default revenue forecast"""
        predictions = []
        for i in range(periods):
            predictions.append({
                'period': i + 1,
                'value': 1000.0,  # Default $1000 per day
                'confidence': 0.5,
                'date': (datetime.now() + timedelta(days=i+1)).isoformat()
            })
        
        return {
            'forecast_type': 'revenue',
            'forecast_periods': periods,
            'predictions': predictions,
            'method': 'default',
            'confidence_level': 0.5,
            'last_updated': datetime.now().isoformat()
        }
    
    def get_inventory_forecast(self, product_id: str, current_stock: int) -> Dict[str, Any]:
        """
        Forecast when inventory will run out based on demand prediction
        
        Args:
            product_id: Product ID
            current_stock: Current stock level
            
        Returns:
            Inventory forecast with stockout prediction
        """
        try:
            # Get demand forecast
            demand_forecast = self.forecast_product_demand(product_id, forecast_periods=90)
            
            if not demand_forecast or 'predictions' not in demand_forecast:
                return {
                    'product_id': product_id,
                    'current_stock': current_stock,
                    'stockout_day': None,
                    'stockout_confidence': None,
                    'cumulative_demand_90_days': 0,
                    'recommended_reorder_quantity': 0,
                    'demand_forecast': demand_forecast
                }
            
            # Calculate cumulative demand
            cumulative_demand = 0
            stockout_day = None
            stockout_confidence = None
            
            for i, prediction in enumerate(demand_forecast['predictions']):
                cumulative_demand += prediction['value']
                if cumulative_demand >= current_stock and stockout_day is None:
                    stockout_day = i + 1
                    stockout_confidence = prediction.get('confidence', 0.5)
            
            return {
                'product_id': product_id,
                'current_stock': current_stock,
                'stockout_day': stockout_day,
                'stockout_confidence': stockout_confidence,
                'cumulative_demand_90_days': cumulative_demand,
                'recommended_reorder_quantity': max(0, cumulative_demand - current_stock),
                'demand_forecast': demand_forecast
            }
            
        except Exception as e:
            logger.error(f"Error getting inventory forecast: {e}")
            return {
                'product_id': product_id,
                'current_stock': current_stock,
                'stockout_day': None,
                'stockout_confidence': None,
                'cumulative_demand_90_days': 0,
                'recommended_reorder_quantity': 0,
                'demand_forecast': {}
            }
