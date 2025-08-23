# Smart E-Commerce Intelligence & Recommendation Engine

**Tagline**: "From raw data to personalized insights, powered by BigQuery Generative AI & Vector Search"

## 🎯 Problem Statement

Modern e-commerce platforms struggle to:

- Provide personalized marketing at scale
- Understand product similarity beyond keywords for substitutions or upsells
- Combine structured product data (price, ratings) with rich media (images) to improve recommendations
- Summarize customer feedback & reviews into actionable insights for product improvements

## 🚀 Solution Overview

This project builds an AI-driven E-Commerce Intelligence Engine on BigQuery that:

1. **Generates hyper-personalized marketing content** for each user
2. **Performs semantic product similarity search** for intelligent recommendations
3. **Combines structured product data with images** for better recommendations and price predictions
4. **Summarizes customer reviews** into key insights for business dashboards
5. **Predicts future product demand** using AI.FORECAST

## 🏗️ Core Components

### 1. Personalized Marketing Engine (AI Architect)

- Uses `AI.GENERATE` to create customized marketing emails
- Based on purchase history, browsing behavior, and demographics

### 2. Semantic Product Search & Smart Substitution (Semantic Detective)

- Converts product descriptions into embeddings using `ML.GENERATE_EMBEDDING`
- Creates `VECTOR INDEX` for fast semantic search
- Uses `VECTOR_SEARCH` to recommend similar products

### 3. Multimodal Product Insights (Multimodal Pioneer)

- Loads product images into Object Tables in BigQuery
- Uses ObjectRef to associate images with products
- Combines structured and unstructured data for better recommendations

### 4. Customer Feedback Summarization

- Uses `AI.GENERATE` on raw review logs
- Creates actionable insights for product improvements

### 5. Demand Forecasting

- Uses `AI.FORECAST` on historical sales data
- Predicts future product demand

## 📁 Project Structure

```
bigquerykaggle/
├── README.md                           # This file
├── requirements.txt                    # Python dependencies
├── config/
│   ├── bigquery_config.py             # BigQuery configuration
│   └── settings.py                    # Project settings
├── data/
│   ├── sample_data/                   # Sample datasets
│   ├── schemas/                       # BigQuery table schemas
│   └── sql/                          # SQL scripts
├── src/
│   ├── __init__.py
│   ├── data_ingestion/               # Data loading utilities
│   ├── ai_engine/                    # AI/ML components
│   ├── vector_search/                # Vector search functionality
│   ├── marketing_engine/             # Marketing content generation
│   └── forecasting/                  # Demand forecasting
├── notebooks/
│   ├── 01_data_exploration.ipynb     # Data exploration
│   ├── 02_ai_engine_demo.ipynb       # AI engine demonstrations
│   ├── 03_vector_search_demo.ipynb   # Vector search examples
│   └── 04_complete_pipeline.ipynb    # End-to-end pipeline
├── tests/
│   └── test_components.py            # Unit tests
└── deployment/
    ├── terraform/                    # Infrastructure as code
    └── docker/                       # Containerization
```

## 🛠️ Setup Instructions

### Prerequisites

- Google Cloud Platform account
- BigQuery enabled
- Vertex AI enabled
- Python 3.8+

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd bigquerykaggle

# Run the setup script
python setup.py
```

### 🔐 Authentication Setup

The project supports multiple authentication methods for Google Cloud:

#### Option 1: Interactive Setup (Recommended)

```bash
python setup_auth.py
```

#### Option 2: Service Account Key File

```bash
# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"

# Or use the helper
python -c "
from config.auth_helper import setup_auth_with_key_file
setup_auth_with_key_file('/path/to/your/service-account-key.json')
"
```

#### Option 3: Service Account JSON String

```bash
# Set environment variable
export GOOGLE_SERVICE_ACCOUNT_JSON='{"type": "service_account", "project_id": "your-project", ...}'

# Or use the helper
python -c "
from config.auth_helper import setup_auth_with_json
setup_auth_with_json('your-service-account-json-string')
"
```

#### Option 4: API Key (for specific API calls)

```bash
# Set environment variable
export GOOGLE_API_KEY="your-api-key"

# Or use the helper
python -c "
from config.auth_helper import setup_auth_with_api_key
setup_auth_with_api_key('your-api-key')
"
```

#### Option 5: Default Application Credentials

```bash
# Install and authenticate with gcloud
gcloud auth application-default login

# Test authentication
python -c "
from config.auth_helper import test_auth
test_auth()
"
```

### 📝 Environment Configuration

Create a `.env` file with your configuration:

```bash
# Copy the template
cp .env.template .env

# Edit with your values
nano .env
```

Example `.env` file:

```env
# Authentication (choose one)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
# OR
GOOGLE_SERVICE_ACCOUNT_JSON={"type": "service_account", ...}
# OR
GOOGLE_API_KEY=your-api-key

# Project Configuration
GOOGLE_CLOUD_PROJECT_ID=your-project-id
BIGQUERY_DATASET=ecommerce_intelligence
BIGQUERY_LOCATION=US
VERTEX_AI_LOCATION=us-central1
STORAGE_BUCKET=your-project-id-ecommerce-images

# AI Model Configuration
TEXT_MODEL=text-bison@001
EMBEDDING_MODEL=textembedding-gecko@001
```

# Install dependencies

pip install -r requirements.txt

# Set up Google Cloud credentials

gcloud auth application-default login

# Configure BigQuery settings

cp config/bigquery_config.py.example config/bigquery_config.py

# Edit config/bigquery_config.py with your project details

````

### Quick Start

```bash
# Run the complete pipeline
python src/main.py

# Or explore individual components
jupyter notebook notebooks/01_data_exploration.ipynb
````

## 📊 Key Features

### 1. Personalized Marketing Engine

- **Input**: User behavior data, purchase history
- **Output**: Customized marketing emails
- **Technology**: BigQuery AI.GENERATE

### 2. Semantic Product Search

- **Input**: Product descriptions, user queries
- **Output**: Similar product recommendations
- **Technology**: BigQuery Vector Search, ML.GENERATE_EMBEDDING

### 3. Multimodal Insights

- **Input**: Product images + structured data
- **Output**: Enhanced recommendations
- **Technology**: BigQuery Object Tables, AI models

### 4. Review Summarization

- **Input**: Customer reviews
- **Output**: Actionable insights
- **Technology**: BigQuery AI.GENERATE

### 5. Demand Forecasting

- **Input**: Historical sales data
- **Output**: Future demand predictions
- **Technology**: BigQuery AI.FORECAST

## 🔧 Configuration

Edit `config/bigquery_config.py` to set your:

- Google Cloud Project ID
- BigQuery Dataset name
- Vertex AI region
- Storage bucket for images

## 📈 Usage Examples

### Generate Personalized Marketing Content

```sql
SELECT user_id,
       AI.GENERATE(
         'Create a marketing email offering personalized discounts for a user who recently purchased ' || last_purchase ||
         ' and browsed ' || recent_category || '.'
       ) AS personalized_email
FROM user_behavior;
```

### Semantic Product Search

```sql
SELECT *
FROM VECTOR_SEARCH(
  TABLE products,
  'embedding',
  (SELECT embedding FROM products WHERE product_id = '123'),
  top_k => 5
);
```

### Review Summarization

```sql
SELECT AI.GENERATE('Summarize the following customer reviews into 3 bullet points: ' || STRING_AGG(review_text, ' '))
FROM reviews
WHERE product_id = '123';
```

### Demand Forecasting

```sql
SELECT AI.FORECAST(sales, 30) AS predicted_sales
FROM sales_data
WHERE product_id = '123';
```

## 🧪 Testing

Run the test suite:

```bash
python -m pytest tests/
```

## 📝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Support

For questions and support:

- Create an issue in the repository
- Check the documentation in the `docs/` folder
- Review the example notebooks in `notebooks/`

## 🚀 Roadmap

- [ ] Real-time streaming data processing
- [ ] Advanced A/B testing framework
- [ ] Multi-language support
- [ ] Mobile app integration
- [ ] Advanced analytics dashboard
- [ ] Machine learning model versioning
- [ ] Automated deployment pipeline
"# BigQuery-AI-Kaggle-2025" 
