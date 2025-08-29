# Data Collection Service

A microservice responsible for collecting financial market data from multiple sources for the AlgoAlchemist trading platform.

## Overview

This service is the single source of truth for all external data ingestion in the AlgoAlchemist ecosystem. It provides reliable, efficient, and intelligent data collection with self-optimization capabilities.

## Features

- Multi-source data collection (Alpaca, Yahoo Finance, Google Sheets)
- Intelligent caching with Redis
- Automatic retries with exponential backoff
- Data quality scoring and validation
- Rate limiting per data source
- Self-optimization based on performance metrics
- REST API for service communication
- Event streaming for real-time updates
- Prometheus metrics for monitoring
- Docker ready for deployment

## Quick Start

### Prerequisites

- Python 3.11+
- Redis (for caching)
- PostgreSQL (for metadata)
- Docker (optional)

### Installation

1. Clone the repository:
```bash
cd /workspaces/data-collection-service
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys
```

5. Run the service:
```bash
uvicorn src.main:app --reload --port 3001
```

## API Documentation

Once running, access the API documentation at:
- Swagger UI: http://localhost:3001/docs
- ReDoc: http://localhost:3001/redoc

## Project Structure

```
data-collection-service/
├── src/                 # Source code
├── tests/              # Test suite
├── docs/               # Documentation
├── scripts/            # Utility scripts
├── config/             # Configuration files
├── logs/               # Application logs
└── requirements.txt    # Dependencies
```

## License

Part of the AlgoAlchemist trading platform.
