# Data Engineering System Design - Setup Guide

## Prerequisites

- Python 3.8+
- Basic understanding of distributed systems
- Familiarity with SQL and Python
- Command line experience

---

## Local Development Environment

### 1. Python Environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip
```

### 2. Install Dependencies

```bash
# Create requirements.txt
cat > requirements.txt << EOF
pyspark==3.5.0
kafka-python==2.0.2
requests==2.31.0
pydantic==2.5.0
great-expectations==0.18.0
pandas==2.1.0
psycopg2-binary==2.9.9
redis==5.0.1
delta-spark==3.0.0
boto3==1.34.0
EOF

# Install
pip install -r requirements.txt
```

---

## Apache Spark Setup

### Option 1: Local Installation

**macOS:**
```bash
brew install apache-spark
```

**Linux:**
```bash
# Download Spark
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark

# Add to PATH
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

**Verify:**
```bash
spark-submit --version
pyspark
```

### Option 2: Docker

```bash
# Pull Spark image
docker pull apache/spark:3.5.0

# Run Spark container
docker run -it --rm \
  -p 4040:4040 \
  -v $(pwd):/workspace \
  apache/spark:3.5.0 \
  /opt/spark/bin/pyspark
```

---

## Apache Kafka Setup

### Option 1: Docker Compose

Create `docker-compose.yml`:
```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Start Kafka:
```bash
docker-compose up -d
```

### Option 2: Local Installation

**macOS:**
```bash
brew install kafka
brew services start zookeeper
brew services start kafka
```

**Create Topic:**
```bash
kafka-topics --create \
  --topic events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

## PostgreSQL Setup

### Docker
```bash
docker run --name postgres-dev \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=dataeng \
  -p 5432:5432 \
  -d postgres:15
```

### Local Installation

**macOS:**
```bash
brew install postgresql@15
brew services start postgresql@15
createdb dataeng
```

**Ubuntu:**
```bash
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
sudo -u postgres createdb dataeng
```

---

## Redis Setup

### Docker
```bash
docker run --name redis-dev \
  -p 6379:6379 \
  -d redis:7
```

### Local Installation

**macOS:**
```bash
brew install redis
brew services start redis
```

---

## Cloud Setup (Optional)

### AWS

**Install AWS CLI:**
```bash
# macOS
brew install awscli

# Linux
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

**Configure:**
```bash
aws configure
# Enter: Access Key, Secret Key, Region, Output format
```

**Create S3 Bucket:**
```bash
aws s3 mb s3://my-data-engineering-bucket
```

### Google Cloud Platform

**Install gcloud:**
```bash
# macOS
brew install google-cloud-sdk

# Linux
curl https://sdk.cloud.google.com | bash
```

**Configure:**
```bash
gcloud init
gcloud auth application-default login
```

**Create GCS Bucket:**
```bash
gsutil mb gs://my-data-engineering-bucket
```

---

## Running the Examples

### 1. Batch vs Streaming

```bash
# Run batch example
python 01_batch_vs_streaming.py

# Or specific function
python -c "from 01_batch_vs_streaming import batch_daily_sales_aggregation; batch_daily_sales_aggregation()"
```

### 2. Data Ingestion

```bash
# Test API ingestion
python -c "from 02_data_ingestion_patterns import api_ingestion_example; api_ingestion_example()"
```

### 3. Fault Tolerance

```bash
# Test retry logic
python -c "from 03_fault_tolerance import fetch_data_from_api; fetch_data_from_api('https://api.example.com/data')"
```

### 4. Idempotency

```bash
# Test idempotent operations
python -c "from 04_idempotency import test_idempotency, deterministic_transformation; test_idempotency(deterministic_transformation, {'id': '1', 'first_name': 'John', 'last_name': 'Doe', 'email': 'john@example.com', 'birth_year': 1990, 'is_premium': True})"
```

### 5. Data Quality

```bash
# Run data quality checks
python -c "from 05_data_quality import example_usage; example_usage()"
```

---

## Jupyter Notebook Setup (Optional)

```bash
# Install Jupyter
pip install jupyter notebook

# Install Spark kernel
pip install spylon-kernel
python -m spylon_kernel install

# Start Jupyter
jupyter notebook
```

---

## IDE Setup

### VS Code

**Extensions:**
- Python
- Jupyter
- Docker
- YAML

**Settings:**
```json
{
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": true,
  "python.formatting.provider": "black"
}
```

### PyCharm

1. Open project
2. Configure Python interpreter (venv)
3. Install Python plugin
4. Configure Spark (if using)

---

## Testing Setup

```bash
# Install testing libraries
pip install pytest pytest-cov pytest-mock

# Create test file
cat > test_example.py << EOF
def test_deterministic_transformation():
    from 04_idempotency import deterministic_transformation
    
    input_data = {
        'id': '1',
        'first_name': 'John',
        'last_name': 'Doe',
        'email': 'john@example.com',
        'birth_year': 1990,
        'is_premium': True
    }
    
    result1 = deterministic_transformation(input_data)
    result2 = deterministic_transformation(input_data)
    
    assert result1 == result2  # Idempotent
EOF

# Run tests
pytest test_example.py -v
```

---

## Monitoring Setup (Optional)

### Prometheus + Grafana

```yaml
# docker-compose-monitoring.yml
version: '3'
services:
  prometheus:
    image: prom/prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
```

---

## Troubleshooting

### Spark Issues

**Issue: OutOfMemoryError**
```bash
# Increase driver memory
spark-submit --driver-memory 4g script.py
```

**Issue: Connection refused**
```bash
# Check Spark is running
jps  # Should show SparkSubmit or similar
```

### Kafka Issues

**Issue: Cannot connect to Kafka**
```bash
# Check Kafka is running
docker ps | grep kafka

# Test connection
kafka-console-producer --topic test --bootstrap-server localhost:9092
```

### Python Issues

**Issue: Module not found**
```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

---

## Learning Path

### Week 1: Fundamentals
- [ ] Read README sections 1-3
- [ ] Set up local environment
- [ ] Run batch vs streaming examples
- [ ] Understand trade-offs

### Week 2: Ingestion & Fault Tolerance
- [ ] Study ingestion patterns
- [ ] Implement retry logic
- [ ] Practice with circuit breakers
- [ ] Set up DLQ

### Week 3: Idempotency & Quality
- [ ] Implement idempotent operations
- [ ] Practice deduplication
- [ ] Set up data quality checks
- [ ] Use Great Expectations

### Week 4: System Design
- [ ] Design end-to-end pipeline
- [ ] Practice interview questions
- [ ] Build portfolio project
- [ ] Review real-world architectures

---

## Practice Projects

### Project 1: Real-Time Analytics Pipeline
- Ingest clickstream data via Kafka
- Process with Spark Streaming
- Store in Delta Lake
- Serve via API

### Project 2: Batch ETL Pipeline
- Extract from PostgreSQL
- Transform with Spark
- Load to data warehouse
- Schedule with Airflow

### Project 3: Data Quality Framework
- Implement comprehensive checks
- Set up monitoring
- Create DLQ handling
- Build alerting

---

## Additional Resources

- **Documentation**: Spark, Kafka, Delta Lake docs
- **Books**: "Designing Data-Intensive Applications"
- **Courses**: Coursera, Udemy data engineering courses
- **Communities**: Reddit r/dataengineering, Stack Overflow

---

## Next Steps

1. Complete environment setup
2. Run all example files
3. Review QUICK_REFERENCE.md
4. Practice with sample data
5. Build your own pipeline

---

**Happy Learning!** 🚀
