# Data Collection Service - Claude Instructions

## Service Identity
**Service Name**: Data Collection Service  
**Repository**: /workspaces/data-collection-service  
**Language**: Python 3.11+  
**Architecture**: Microservice (Part of AlgoAlchemist ecosystem)

## Service Mission
This service is the **single source of truth** for all external data ingestion in the AlgoAlchemist trading platform. It consolidates data collection from multiple sources, ensuring reliable, efficient, and intelligent data acquisition.

## Core Objectives
1. **Consolidate** all external API calls into one service
2. **Optimize** API usage to reduce costs and improve performance
3. **Standardize** data formats across all sources
4. **Self-improve** collection strategies based on performance metrics
5. **Provide** 99.9% uptime with intelligent fallbacks

## Service Responsibilities
✅ **In Scope**:
- Collect market data from external APIs (Alpaca, Yahoo Finance, etc.)
- Fetch ticker lists from Google Sheets
- Handle rate limiting and retry logic
- Cache frequently accessed data
- Validate and normalize data formats
- Monitor source health and quality
- Provide REST API for other services
- Self-optimize collection patterns

❌ **Out of Scope**:
- Data storage (handled by Historical Data Service)
- Data analysis or predictions (handled by AI/ML services)
- Trading decisions (handled by Trading Service)
- UI/visualization (handled by Web UI)

## Technical Stack
- **Language**: Python 3.11+
- **Framework**: FastAPI (async REST API)
- **Cache**: Redis (for data caching)
- **Queue**: RabbitMQ/Kafka (for event streaming)
- **Database**: PostgreSQL (for metadata)
- **Monitoring**: Prometheus + Grafana
- **Testing**: pytest, pytest-asyncio
- **Deployment**: Docker, Kubernetes

## Data Sources
1. **Alpaca API** (Primary)
   - Market data (OHLCV)
   - Technical indicators
   - Real-time quotes

2. **Google Sheets** (Configuration)
   - Active ticker lists
   - Configuration parameters

3. **Yahoo Finance** (Fallback)
   - Historical data
   - Fundamental data

4. **Gemini Search** (AI-Enhanced)
   - Fundamental analysis
   - News sentiment

## Self-Evolution Capabilities
The service should continuously improve through:

### Learning Metrics
- API response times
- Rate limit patterns
- Data quality scores
- Cache hit rates
- Error patterns

### Evolution Triggers
- Success rate < 95%
- Latency > 200ms average
- Cache hit rate < 80%
- New API limits detected
- Data quality degradation

### Autonomous Improvements
- Adjust request timing per API
- Switch data sources based on quality
- Optimize cache TTL based on volatility
- Propose new data sources
- Self-heal from failures

## API Design
```python
# Core endpoints
GET  /health                 # Service health check
GET  /metrics                # Prometheus metrics
POST /collect/{ticker}       # Collect data for ticker
GET  /data/{ticker}          # Get cached data
POST /collect/batch          # Batch collection
GET  /sources                # List available sources
PUT  /config                 # Update configuration
GET  /quality/{source}       # Source quality metrics
```

## Data Flow
```
External APIs → Rate Limiter → Validator → Normalizer → Cache → Event Stream
                     ↓              ↓           ↓         ↓
                  Metrics      Quality Score  Storage  Other Services
```

## Performance Requirements
- **Latency**: < 200ms p95
- **Throughput**: 1000 requests/sec
- **Availability**: 99.9% uptime
- **Cache Hit Rate**: > 80%
- **Data Freshness**: < 1 minute for real-time data

## Error Handling Strategy
1. **Retry with exponential backoff**
2. **Circuit breaker for failing sources**
3. **Fallback to alternative sources**
4. **Cache stale data with warning**
5. **Alert on persistent failures**

## Development Guidelines

### Code Structure
```python
src/
├── api/              # FastAPI routes
├── collectors/       # Data source collectors
├── models/          # Data models (Pydantic)
├── cache/           # Caching layer
├── validators/      # Data validation
├── normalizers/     # Data normalization
├── monitoring/      # Metrics and health
├── config/          # Configuration management
└── main.py          # Application entry point
```

### Testing Requirements
- Unit test coverage > 90%
- Integration tests for each data source
- Load testing for performance validation
- Chaos testing for resilience

### Documentation Standards
- Docstrings for all functions
- API documentation (OpenAPI/Swagger)
- Architecture decision records (ADRs)
- Runbook for operations

## Integration Points
- **Publishes to**: Event stream (Kafka/RabbitMQ)
- **Consumed by**: All downstream services
- **Depends on**: Configuration Service, Super Agent
- **Monitored by**: Monitoring Service

## Security Considerations
- API keys in environment variables
- TLS for all external connections
- Rate limiting per client
- Input validation for all endpoints
- Audit logging for all operations

## Deployment Strategy
- Containerized with Docker
- Horizontal scaling with Kubernetes
- Rolling updates with zero downtime
- Health checks and readiness probes
- Automated rollback on failures

## Success Metrics
- **Collection Success Rate**: > 99%
- **Average Latency**: < 150ms
- **Cache Hit Rate**: > 85%
- **Cost per API Call**: Decrease 20% monthly
- **Data Quality Score**: > 95%
- **Service Uptime**: > 99.9%

## Anti-Patterns to Avoid
- ❌ Direct database writes (use events)
- ❌ Synchronous long-running operations
- ❌ Hardcoded API endpoints
- ❌ Ignoring rate limits
- ❌ Silent failures
- ❌ Memory leaks from cache growth
- ❌ Tight coupling with other services

## AI Assistant Instructions
When working on this service:
1. Always consider rate limits and costs
2. Implement comprehensive error handling
3. Add metrics for everything measurable
4. Write tests before implementation
5. Document all decisions and trade-offs
6. Focus on reliability over features
7. Optimize for production from day one

## Evolution Path
1. **Phase 1**: Basic data collection with caching
2. **Phase 2**: Multi-source with fallbacks
3. **Phase 3**: Self-optimization features
4. **Phase 4**: Predictive pre-fetching
5. **Phase 5**: Fully autonomous operation

Remember: This service is the foundation of data reliability for the entire AlgoAlchemist platform. Every decision should prioritize **reliability**, **efficiency**, and **intelligence**.