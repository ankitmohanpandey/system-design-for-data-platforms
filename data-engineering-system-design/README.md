# Data Engineering System Design Complete Guide

## Table of Contents
1. [What is Data Engineering System Design?](#what-is-data-engineering-system-design)
2. [Core Principles](#core-principles)
3. [Batch vs Streaming Systems](#batch-vs-streaming-systems)
4. [Data Ingestion Patterns](#data-ingestion-patterns)
5. [Fault Tolerance](#fault-tolerance)
6. [Idempotency](#idempotency)
7. [Data Quality Checks](#data-quality-checks)
8. [Real-World Architectures](#real-world-architectures)
9. [Best Practices](#best-practices)
10. [Interview Preparation](#interview-preparation)

---

## What is Data Engineering System Design?

**Data Engineering System Design** is the process of architecting scalable, reliable, and efficient data systems that can handle large volumes of data while maintaining quality, consistency, and performance.

### Key Objectives
- **Scalability**: Handle growing data volumes
- **Reliability**: Ensure data accuracy and availability
- **Performance**: Process data efficiently
- **Maintainability**: Easy to operate and debug
- **Cost-Effectiveness**: Optimize resource usage

### Why It Matters
✅ **Business Impact**: Data drives decisions  
✅ **Scale**: Modern systems process petabytes daily  
✅ **Complexity**: Multiple sources, formats, and consumers  
✅ **Real-Time Needs**: Immediate insights required  
✅ **Data Quality**: Garbage in, garbage out  

---

## Core Principles

### 1. CAP Theorem
In distributed systems, you can only guarantee 2 of 3:
- **Consistency**: All nodes see the same data
- **Availability**: System always responds
- **Partition Tolerance**: System works despite network failures

**Data Engineering Choice**: Usually CP or AP
- **CP**: Banking, financial transactions (consistency critical)
- **AP**: Analytics, recommendations (availability critical)

### 2. Data Pipeline Stages

```
Source → Ingestion → Processing → Storage → Consumption
   ↓         ↓           ↓           ↓          ↓
 APIs    Kafka/Pub   Spark/Flink  S3/HDFS   BI Tools
 DBs     Sub/Kinesis  Airflow      BigQuery  ML Models
 Files   Firehose     dbt          Snowflake  APIs
```

### 3. Lambda Architecture

```
                    ┌─────────────┐
                    │   Sources   │
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              │                         │
        ┌─────▼─────┐            ┌─────▼─────┐
        │   Batch   │            │ Streaming │
        │   Layer   │            │   Layer   │
        └─────┬─────┘            └─────┬─────┘
              │                         │
              │    ┌─────────────┐     │
              └────►  Serving    ◄─────┘
                   │   Layer     │
                   └─────┬───────┘
                         │
                    ┌────▼────┐
                    │  Users  │
                    └─────────┘
```

### 4. Kappa Architecture

```
Source → Stream Processing → Serving Layer → Users
           (Everything is a stream)
```

---

## Batch vs Streaming Systems

### Batch Processing

**Definition**: Process large volumes of data at scheduled intervals.

**Characteristics:**
- High throughput
- High latency (minutes to hours)
- Complete dataset available
- Easier to implement
- Cost-effective for large volumes

**Use Cases:**
- Daily/weekly reports
- Historical analysis
- ETL jobs
- Model training
- Data warehouse loads

**Technologies:**
- Apache Spark (batch mode)
- Apache Hadoop MapReduce
- AWS Batch
- Google Dataflow (batch)
- dbt

**Example: Daily Sales Report**
```python
# Batch job runs at midnight
def daily_sales_batch():
    # Read yesterday's data
    sales = spark.read.parquet(f"s3://sales/{yesterday}")
    
    # Aggregate
    daily_summary = sales.groupBy("store_id", "product_id") \
        .agg(
            sum("amount").alias("total_sales"),
            count("*").alias("num_transactions")
        )
    
    # Write to warehouse
    daily_summary.write.mode("overwrite") \
        .parquet(f"s3://warehouse/daily_sales/{yesterday}")
```

**Advantages:**
✅ Simple to implement and debug  
✅ Can reprocess entire dataset  
✅ Efficient for large volumes  
✅ Lower infrastructure cost  

**Disadvantages:**
❌ High latency  
❌ Not suitable for real-time needs  
❌ Resource spikes during processing  

### Streaming Processing

**Definition**: Process data continuously as it arrives.

**Characteristics:**
- Low latency (milliseconds to seconds)
- Continuous processing
- Handles unbounded data
- More complex to implement
- Higher operational cost

**Use Cases:**
- Real-time dashboards
- Fraud detection
- Recommendation engines
- IoT sensor processing
- Log monitoring

**Technologies:**
- Apache Kafka Streams
- Apache Flink
- Apache Spark Streaming
- AWS Kinesis
- Google Dataflow (streaming)

**Example: Real-Time Fraud Detection**
```python
# Streaming job processes events continuously
def fraud_detection_stream():
    # Read from Kafka
    transactions = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .load()
    
    # Detect anomalies in real-time
    flagged = transactions \
        .filter(is_suspicious) \
        .select("transaction_id", "amount", "user_id")
    
    # Write alerts
    flagged.writeStream \
        .format("kafka") \
        .option("topic", "fraud_alerts") \
        .start()
```

**Advantages:**
✅ Low latency  
✅ Real-time insights  
✅ Continuous processing  
✅ Event-driven architecture  

**Disadvantages:**
❌ Complex to implement  
❌ Harder to debug  
❌ Higher operational cost  
❌ State management challenges  

### Batch vs Streaming Comparison

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| **Latency** | Hours/Minutes | Seconds/Milliseconds |
| **Data Volume** | Large bounded datasets | Unbounded continuous data |
| **Complexity** | Lower | Higher |
| **Cost** | Lower | Higher |
| **Use Case** | Historical analysis | Real-time monitoring |
| **Reprocessing** | Easy | Complex |
| **State Management** | Simple | Complex |
| **Debugging** | Easier | Harder |

### Hybrid Approach (Lambda Architecture)

Combine both for best of both worlds:

```python
# Batch Layer: Accurate historical data
def batch_layer():
    # Process complete dataset daily
    historical_data = spark.read.parquet("s3://data/")
    aggregates = compute_accurate_metrics(historical_data)
    aggregates.write.mode("overwrite").save("s3://batch_view/")

# Speed Layer: Real-time approximate data
def speed_layer():
    # Process recent data in real-time
    stream = spark.readStream.format("kafka").load()
    recent_aggregates = compute_approximate_metrics(stream)
    recent_aggregates.writeStream.save("s3://speed_view/")

# Serving Layer: Merge both views
def serve_query(user_query):
    batch_result = read_batch_view(user_query)
    speed_result = read_speed_view(user_query)
    return merge_results(batch_result, speed_result)
```

### When to Use What?

**Choose Batch When:**
- Latency requirements > 1 hour
- Processing complete datasets
- Cost is a major concern
- Simpler operations preferred
- Historical analysis

**Choose Streaming When:**
- Latency requirements < 1 minute
- Real-time alerts needed
- Event-driven architecture
- Continuous monitoring
- Immediate action required

**Choose Hybrid When:**
- Need both real-time and accurate historical data
- Complex aggregations with low latency
- Serving layer can merge results

---

## Data Ingestion Patterns

### 1. Full Load (Snapshot)

**Pattern**: Load complete dataset each time.

**When to Use:**
- Small datasets
- Data changes frequently
- Simple implementation needed

**Example:**
```python
def full_load():
    # Extract entire table
    data = source_db.read_table("customers")
    
    # Load to target (overwrite)
    data.write.mode("overwrite").save("s3://warehouse/customers/")
```

**Pros:** Simple, always in sync  
**Cons:** Inefficient for large tables, high load on source

### 2. Incremental Load (CDC)

**Pattern**: Load only changed data since last run.

**Approaches:**

**A. Timestamp-Based**
```python
def incremental_load_timestamp():
    # Get last processed timestamp
    last_ts = get_last_watermark()
    
    # Extract only new/updated records
    new_data = source_db.query(f"""
        SELECT * FROM orders 
        WHERE updated_at > '{last_ts}'
    """)
    
    # Append to target
    new_data.write.mode("append").save("s3://warehouse/orders/")
    
    # Update watermark
    update_watermark(current_timestamp)
```

**B. Change Data Capture (CDC)**
```python
# Using Debezium or database logs
def cdc_ingestion():
    # Read from CDC stream (Kafka)
    changes = spark.readStream \
        .format("kafka") \
        .option("subscribe", "db.customers.changes") \
        .load()
    
    # Process INSERT, UPDATE, DELETE
    processed = changes.select(
        col("op").alias("operation"),  # I, U, D
        from_json(col("value"), schema).alias("data")
    )
    
    # Apply changes to target
    apply_changes(processed)
```

**C. Sequence-Based**
```python
def incremental_load_sequence():
    last_id = get_last_processed_id()
    
    new_records = source_db.query(f"""
        SELECT * FROM transactions 
        WHERE id > {last_id}
        ORDER BY id
    """)
    
    new_records.write.mode("append").save("s3://warehouse/transactions/")
```

**Pros:** Efficient, low source load  
**Cons:** Requires tracking mechanism, more complex

### 3. Streaming Ingestion

**Pattern**: Continuous real-time data ingestion.

**Technologies:**
- Apache Kafka
- AWS Kinesis
- Google Pub/Sub
- Azure Event Hubs

**Example: Kafka Ingestion**
```python
def kafka_ingestion():
    # Producer: Send events to Kafka
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    for event in event_stream():
        producer.send('events', 
                     key=event['user_id'],
                     value=json.dumps(event))
    
    # Consumer: Read from Kafka
    consumer = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "events") \
        .load()
    
    # Process and write
    consumer.writeStream \
        .format("parquet") \
        .option("path", "s3://data/events/") \
        .option("checkpointLocation", "s3://checkpoints/") \
        .start()
```

### 4. API-Based Ingestion

**Pattern**: Pull data from REST APIs.

**Example:**
```python
import requests
from datetime import datetime, timedelta

def api_ingestion():
    # Pagination
    page = 1
    all_data = []
    
    while True:
        response = requests.get(
            f"https://api.example.com/data",
            params={
                'page': page,
                'start_date': (datetime.now() - timedelta(days=1)).isoformat(),
                'end_date': datetime.now().isoformat()
            },
            headers={'Authorization': f'Bearer {API_TOKEN}'}
        )
        
        data = response.json()
        if not data['results']:
            break
            
        all_data.extend(data['results'])
        page += 1
        
        # Rate limiting
        time.sleep(1)
    
    # Write to storage
    df = pd.DataFrame(all_data)
    df.to_parquet('s3://raw/api_data.parquet')
```

### 5. File-Based Ingestion

**Pattern**: Ingest files from storage systems.

**Example: S3 File Watcher**
```python
def file_ingestion():
    # Watch for new files
    new_files = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "s3://schemas/") \
        .load("s3://incoming/")
    
    # Process files
    processed = new_files.select(
        col("*"),
        current_timestamp().alias("ingestion_time")
    )
    
    # Write to processed zone
    processed.writeStream \
        .format("delta") \
        .option("checkpointLocation", "s3://checkpoints/") \
        .start("s3://processed/")
```

### 6. Database Replication

**Pattern**: Replicate entire database.

**Tools:**
- AWS DMS (Database Migration Service)
- Fivetran
- Airbyte
- Debezium

**Example: AWS DMS**
```python
# Configuration for DMS
replication_config = {
    "source": {
        "engine": "postgres",
        "endpoint": "source-db.example.com",
        "database": "production"
    },
    "target": {
        "engine": "s3",
        "bucket": "data-lake",
        "format": "parquet"
    },
    "replication_type": "cdc",  # or "full-load"
    "tables": ["orders", "customers", "products"]
}
```

### Ingestion Pattern Selection

| Pattern | Latency | Complexity | Cost | Use Case |
|---------|---------|------------|------|----------|
| **Full Load** | High | Low | High | Small tables |
| **Incremental** | Medium | Medium | Low | Large tables |
| **Streaming** | Low | High | High | Real-time |
| **API** | Medium | Medium | Medium | External data |
| **File** | Medium | Low | Low | Batch files |
| **Replication** | Low | Low | Medium | Database sync |

---

## Fault Tolerance

### What is Fault Tolerance?

**Definition**: System's ability to continue operating properly in the event of failures.

### Types of Failures

1. **Transient Failures**: Temporary issues (network blip)
2. **Permanent Failures**: Persistent issues (disk failure)
3. **Partial Failures**: Some components fail, others work

### Fault Tolerance Strategies

### 1. Retry Logic

**Pattern**: Automatically retry failed operations.

**Example: Exponential Backoff**
```python
import time
from functools import wraps

def retry_with_backoff(max_retries=3, base_delay=1, max_delay=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        raise
                    
                    # Exponential backoff: 1s, 2s, 4s, 8s, ...
                    delay = min(base_delay * (2 ** (retries - 1)), max_delay)
                    print(f"Retry {retries}/{max_retries} after {delay}s. Error: {e}")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry_with_backoff(max_retries=5)
def fetch_data_from_api():
    response = requests.get("https://api.example.com/data")
    response.raise_for_status()
    return response.json()
```

**Best Practices:**
- Use exponential backoff
- Add jitter to prevent thundering herd
- Set maximum retry limit
- Log all retry attempts

### 2. Checkpointing

**Pattern**: Save progress to resume from last successful point.

**Example: Spark Structured Streaming**
```python
def streaming_with_checkpoint():
    # Checkpoint enables fault tolerance
    query = spark.readStream \
        .format("kafka") \
        .load() \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "s3://checkpoints/app1/") \
        .start("s3://output/")
    
    # If job fails, it resumes from checkpoint
    query.awaitTermination()
```

**Checkpoint Contents:**
- Offset information (where to resume)
- State information (aggregations, windows)
- Metadata (schema, configuration)

### 3. Dead Letter Queue (DLQ)

**Pattern**: Store failed messages for later processing.

**Example:**
```python
def process_with_dlq(message):
    try:
        # Process message
        result = process_message(message)
        
        # Write to main output
        write_to_output(result)
        
    except ValidationError as e:
        # Send to DLQ for invalid data
        send_to_dlq(message, error=str(e), queue="validation_errors")
        
    except TransientError as e:
        # Retry transient errors
        retry_queue.send(message)
        
    except Exception as e:
        # Unknown errors to DLQ
        send_to_dlq(message, error=str(e), queue="unknown_errors")
```

**DLQ Processing:**
```python
def process_dlq():
    # Periodically review DLQ
    dlq_messages = read_from_dlq()
    
    for msg in dlq_messages:
        # Fix data issues
        fixed_msg = fix_message(msg)
        
        # Reprocess
        if fixed_msg:
            reprocess(fixed_msg)
        else:
            # Move to permanent failure storage
            archive_failed_message(msg)
```

### 4. Circuit Breaker

**Pattern**: Prevent cascading failures by stopping calls to failing services.

**Example:**
```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.last_failure_time = None
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
            
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
            
            raise

# Usage
breaker = CircuitBreaker(failure_threshold=3, timeout=30)

def fetch_data():
    return breaker.call(requests.get, "https://api.example.com/data")
```

### 5. Replication

**Pattern**: Maintain multiple copies of data.

**Example: Kafka Replication**
```python
# Kafka topic with replication factor 3
kafka-topics --create \
    --topic critical-events \
    --partitions 10 \
    --replication-factor 3 \
    --config min.insync.replicas=2

# Producer with acknowledgment
producer = KafkaProducer(
    acks='all',  # Wait for all replicas
    retries=3
)
```

### 6. Graceful Degradation

**Pattern**: Reduce functionality instead of complete failure.

**Example:**
```python
def get_recommendations(user_id):
    try:
        # Try ML model
        return ml_model.predict(user_id)
    except ModelServiceDown:
        # Fallback to rule-based
        return rule_based_recommendations(user_id)
    except Exception:
        # Fallback to popular items
        return get_popular_items()
```

### 7. Bulkhead Pattern

**Pattern**: Isolate resources to prevent total system failure.

**Example:**
```python
from concurrent.futures import ThreadPoolExecutor

# Separate thread pools for different operations
critical_pool = ThreadPoolExecutor(max_workers=10)
non_critical_pool = ThreadPoolExecutor(max_workers=5)

def process_critical_task(task):
    return critical_pool.submit(handle_critical, task)

def process_non_critical_task(task):
    return non_critical_pool.submit(handle_non_critical, task)
```

### Fault Tolerance Checklist

✅ **Retry Logic**: Implement with exponential backoff  
✅ **Checkpointing**: Save progress regularly  
✅ **Dead Letter Queue**: Handle failed messages  
✅ **Circuit Breaker**: Prevent cascading failures  
✅ **Replication**: Multiple copies of critical data  
✅ **Monitoring**: Alert on failures  
✅ **Graceful Degradation**: Fallback mechanisms  
✅ **Timeouts**: Prevent hanging operations  

---

## Idempotency

### What is Idempotency?

**Definition**: An operation that produces the same result no matter how many times it's executed.

**Mathematical**: `f(f(x)) = f(x)`

### Why Idempotency Matters

- **Retries**: Safe to retry failed operations
- **Exactly-Once**: Achieve exactly-once semantics
- **Data Consistency**: Prevent duplicate data
- **Fault Tolerance**: Recover from failures safely

### Non-Idempotent vs Idempotent

**Non-Idempotent (BAD):**
```python
# Running twice creates duplicates
def process_order(order_id):
    order = get_order(order_id)
    inventory.decrement(order.product_id, order.quantity)  # ❌ Runs twice = double decrement
    db.insert(order)  # ❌ Runs twice = duplicate records
```

**Idempotent (GOOD):**
```python
# Running multiple times has same effect
def process_order(order_id):
    # Check if already processed
    if db.exists(order_id):
        return  # ✅ Already processed, skip
    
    order = get_order(order_id)
    inventory.decrement(order.product_id, order.quantity)
    db.insert_with_unique_constraint(order)  # ✅ Unique constraint prevents duplicates
```

### Idempotency Patterns

### 1. Unique Keys / Deduplication

**Pattern**: Use unique identifiers to prevent duplicates.

**Example: Database Upsert**
```python
def upsert_customer(customer_data):
    # UPSERT is idempotent
    db.execute("""
        INSERT INTO customers (customer_id, name, email, updated_at)
        VALUES (%(id)s, %(name)s, %(email)s, %(updated_at)s)
        ON CONFLICT (customer_id) 
        DO UPDATE SET 
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            updated_at = EXCLUDED.updated_at
    """, customer_data)
```

**Example: Spark Deduplication**
```python
def deduplicate_events():
    # Read events
    events = spark.read.parquet("s3://events/")
    
    # Deduplicate by event_id (keep latest)
    deduped = events \
        .withColumn("row_num", 
                   row_number().over(
                       Window.partitionBy("event_id")
                             .orderBy(desc("timestamp"))
                   )) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    # Overwrite mode is idempotent
    deduped.write.mode("overwrite").parquet("s3://events_deduped/")
```

### 2. Idempotency Keys

**Pattern**: Client generates unique key for each request.

**Example: Payment Processing**
```python
def process_payment(payment_data, idempotency_key):
    # Check if already processed
    existing = db.query(
        "SELECT * FROM payments WHERE idempotency_key = %s",
        (idempotency_key,)
    )
    
    if existing:
        # Return cached result
        return existing[0]
    
    # Process payment
    result = payment_gateway.charge(payment_data)
    
    # Store with idempotency key
    db.execute("""
        INSERT INTO payments (idempotency_key, amount, status, result)
        VALUES (%s, %s, %s, %s)
    """, (idempotency_key, payment_data['amount'], 'completed', result))
    
    return result

# Client usage
idempotency_key = str(uuid.uuid4())
process_payment(payment_data, idempotency_key)
# Retry with same key is safe
process_payment(payment_data, idempotency_key)  # Returns same result
```

### 3. State Checks

**Pattern**: Check current state before applying changes.

**Example: State Machine**
```python
def transition_order_status(order_id, new_status):
    order = db.get_order(order_id)
    
    # Define valid transitions
    valid_transitions = {
        'pending': ['confirmed', 'cancelled'],
        'confirmed': ['shipped', 'cancelled'],
        'shipped': ['delivered'],
        'delivered': [],
        'cancelled': []
    }
    
    # Check if transition is valid
    if new_status not in valid_transitions.get(order.status, []):
        # Idempotent: already in desired state or invalid transition
        if order.status == new_status:
            return order  # Already in desired state
        else:
            raise InvalidTransition(f"Cannot transition from {order.status} to {new_status}")
    
    # Apply transition
    db.execute("""
        UPDATE orders 
        SET status = %s, updated_at = NOW()
        WHERE order_id = %s AND status = %s
    """, (new_status, order_id, order.status))
    
    return db.get_order(order_id)
```

### 4. Deterministic Processing

**Pattern**: Same input always produces same output.

**Example: Data Transformation**
```python
def transform_data(input_data):
    # ✅ Deterministic: same input = same output
    result = {
        'user_id': input_data['id'],
        'full_name': f"{input_data['first_name']} {input_data['last_name']}",
        'email_domain': input_data['email'].split('@')[1],
        'created_date': input_data['created_at'].split('T')[0]
    }
    return result

# ❌ Non-deterministic: includes timestamp
def transform_data_bad(input_data):
    result = {
        'user_id': input_data['id'],
        'processed_at': datetime.now(),  # ❌ Changes each run
        'random_id': uuid.uuid4()  # ❌ Changes each run
    }
    return result
```

### 5. Versioning

**Pattern**: Use version numbers to track changes.

**Example: Optimistic Locking**
```python
def update_record(record_id, new_data, expected_version):
    # Update only if version matches
    result = db.execute("""
        UPDATE records 
        SET data = %s, 
            version = version + 1,
            updated_at = NOW()
        WHERE record_id = %s 
          AND version = %s
    """, (new_data, record_id, expected_version))
    
    if result.rowcount == 0:
        # Version mismatch or record doesn't exist
        current = db.get_record(record_id)
        if current.version > expected_version:
            # Already updated, idempotent
            return current
        else:
            raise ConcurrentModificationError()
    
    return db.get_record(record_id)
```

### 6. Exactly-Once Semantics

**Pattern**: Combine idempotent writes with transactional reads.

**Example: Kafka + Database**
```python
def exactly_once_processing():
    # Kafka consumer with exactly-once semantics
    consumer = KafkaConsumer(
        'events',
        enable_auto_commit=False,
        isolation_level='read_committed'
    )
    
    for message in consumer:
        # Start transaction
        with db.transaction():
            # Check if already processed
            if db.exists('processed_offsets', 
                        partition=message.partition, 
                        offset=message.offset):
                continue
            
            # Process message (idempotent)
            result = process_message(message.value)
            
            # Write result
            db.upsert('results', result)
            
            # Record offset
            db.insert('processed_offsets', {
                'partition': message.partition,
                'offset': message.offset,
                'processed_at': datetime.now()
            })
            
            # Commit transaction and offset together
            consumer.commit()
```

### Idempotency Checklist

✅ **Unique Constraints**: Prevent duplicate inserts  
✅ **Upsert Operations**: Use INSERT ... ON CONFLICT  
✅ **Idempotency Keys**: Client-generated unique IDs  
✅ **State Checks**: Verify before applying changes  
✅ **Deterministic Logic**: No random values or timestamps  
✅ **Deduplication**: Remove duplicates before processing  
✅ **Transactional**: Atomic operations  
✅ **Versioning**: Track changes with version numbers  

---

## Data Quality Checks

### What is Data Quality?

**Definition**: The degree to which data meets requirements for its intended use.

### Dimensions of Data Quality

1. **Accuracy**: Data is correct
2. **Completeness**: All required data is present
3. **Consistency**: Data is consistent across systems
4. **Timeliness**: Data is up-to-date
5. **Validity**: Data conforms to rules
6. **Uniqueness**: No duplicates

### Data Quality Framework

```
Source → Validation → Processing → Quality Checks → Storage
   ↓         ↓            ↓              ↓            ↓
 Raw     Schema      Transform      Metrics      Curated
 Data    Check       Data           Alerts       Data
```

### Quality Check Types

### 1. Schema Validation

**Pattern**: Ensure data matches expected schema.

**Example: Pydantic Validation**
```python
from pydantic import BaseModel, validator, EmailStr
from datetime import datetime
from typing import Optional

class UserEvent(BaseModel):
    user_id: str
    email: EmailStr
    age: int
    signup_date: datetime
    country: str
    
    @validator('age')
    def age_must_be_valid(cls, v):
        if v < 0 or v > 150:
            raise ValueError('Age must be between 0 and 150')
        return v
    
    @validator('country')
    def country_must_be_valid(cls, v):
        valid_countries = ['US', 'UK', 'CA', 'AU']
        if v not in valid_countries:
            raise ValueError(f'Country must be one of {valid_countries}')
        return v

# Usage
def validate_event(event_data):
    try:
        event = UserEvent(**event_data)
        return event, None
    except Exception as e:
        return None, str(e)

# Process events
for raw_event in events:
    validated, error = validate_event(raw_event)
    if error:
        send_to_dlq(raw_event, error)
    else:
        process_event(validated)
```

**Example: Spark Schema Enforcement**
```python
from pyspark.sql.types import *

# Define expected schema
expected_schema = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("signup_date", TimestampType(), nullable=False),
    StructField("country", StringType(), nullable=False)
])

# Read with schema enforcement
df = spark.read \
    .schema(expected_schema) \
    .option("mode", "DROPMALFORMED") \
    .json("s3://raw/events/")

# Or fail on schema mismatch
df = spark.read \
    .schema(expected_schema) \
    .option("mode", "FAILFAST") \
    .json("s3://raw/events/")
```

### 2. Null Checks

**Pattern**: Ensure required fields are not null.

**Example:**
```python
def check_nulls(df):
    # Check for nulls in critical columns
    critical_columns = ['user_id', 'email', 'transaction_id']
    
    for col_name in critical_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        total_count = df.count()
        null_pct = (null_count / total_count) * 100
        
        if null_pct > 0:
            alert(f"Column {col_name} has {null_pct:.2f}% null values")
            
        if null_pct > 5:  # Threshold
            raise DataQualityError(f"Too many nulls in {col_name}")
    
    return df
```

### 3. Range Checks

**Pattern**: Ensure values are within expected ranges.

**Example:**
```python
def validate_ranges(df):
    quality_checks = {
        'age': (0, 150),
        'price': (0, 1000000),
        'quantity': (1, 10000),
        'discount_pct': (0, 100)
    }
    
    for col_name, (min_val, max_val) in quality_checks.items():
        # Count out-of-range values
        invalid = df.filter(
            (col(col_name) < min_val) | (col(col_name) > max_val)
        ).count()
        
        if invalid > 0:
            # Log invalid records
            df.filter(
                (col(col_name) < min_val) | (col(col_name) > max_val)
            ).write.mode("append").parquet(f"s3://dlq/{col_name}_range_errors/")
            
            alert(f"{invalid} records have invalid {col_name}")
    
    # Filter to valid ranges
    for col_name, (min_val, max_val) in quality_checks.items():
        df = df.filter(
            (col(col_name) >= min_val) & (col(col_name) <= max_val)
        )
    
    return df
```

### 4. Uniqueness Checks

**Pattern**: Ensure no duplicates in unique columns.

**Example:**
```python
def check_duplicates(df, unique_columns):
    # Check for duplicates
    total_count = df.count()
    unique_count = df.select(unique_columns).distinct().count()
    
    duplicate_count = total_count - unique_count
    
    if duplicate_count > 0:
        # Find duplicates
        duplicates = df.groupBy(unique_columns) \
            .count() \
            .filter(col("count") > 1)
        
        # Log duplicates
        duplicates.write.mode("overwrite") \
            .parquet("s3://quality/duplicates/")
        
        alert(f"Found {duplicate_count} duplicate records")
        
        # Deduplicate (keep first occurrence)
        window = Window.partitionBy(unique_columns).orderBy("timestamp")
        df = df.withColumn("row_num", row_number().over(window)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
    
    return df
```

### 5. Referential Integrity

**Pattern**: Ensure foreign keys exist in referenced tables.

**Example:**
```python
def check_referential_integrity(orders_df, customers_df):
    # Check if all customer_ids in orders exist in customers
    valid_customer_ids = customers_df.select("customer_id").distinct()
    
    # Find orphaned orders
    orphaned = orders_df.join(
        valid_customer_ids,
        on="customer_id",
        how="left_anti"  # Orders without matching customer
    )
    
    orphaned_count = orphaned.count()
    
    if orphaned_count > 0:
        # Log orphaned records
        orphaned.write.mode("append") \
            .parquet("s3://quality/orphaned_orders/")
        
        alert(f"Found {orphaned_count} orders with invalid customer_id")
        
        # Option 1: Filter out orphaned records
        orders_df = orders_df.join(valid_customer_ids, on="customer_id", how="inner")
        
        # Option 2: Create placeholder customer
        # orders_df = handle_orphaned_orders(orders_df, orphaned)
    
    return orders_df
```

### 6. Freshness Checks

**Pattern**: Ensure data is recent enough.

**Example:**
```python
from datetime import datetime, timedelta

def check_data_freshness(df, timestamp_column, max_age_hours=24):
    # Get latest timestamp in data
    latest_timestamp = df.agg(max(col(timestamp_column))).collect()[0][0]
    
    # Calculate age
    age = datetime.now() - latest_timestamp
    age_hours = age.total_seconds() / 3600
    
    if age_hours > max_age_hours:
        alert(f"Data is {age_hours:.1f} hours old (threshold: {max_age_hours})")
        raise DataFreshnessError(f"Data too old: {age_hours:.1f} hours")
    
    # Filter out old data
    cutoff = datetime.now() - timedelta(hours=max_age_hours)
    df = df.filter(col(timestamp_column) >= cutoff)
    
    return df
```

### 7. Statistical Checks

**Pattern**: Detect anomalies using statistical methods.

**Example:**
```python
def statistical_quality_checks(df, column_name):
    # Calculate statistics
    stats = df.select(
        mean(col(column_name)).alias("mean"),
        stddev(col(column_name)).alias("stddev"),
        min(col(column_name)).alias("min"),
        max(col(column_name)).alias("max"),
        count(col(column_name)).alias("count")
    ).collect()[0]
    
    # Check for anomalies (values > 3 standard deviations)
    anomaly_threshold = stats['mean'] + (3 * stats['stddev'])
    
    anomalies = df.filter(col(column_name) > anomaly_threshold)
    anomaly_count = anomalies.count()
    
    if anomaly_count > 0:
        # Log anomalies
        anomalies.write.mode("append") \
            .parquet(f"s3://quality/anomalies/{column_name}/")
        
        alert(f"Found {anomaly_count} anomalies in {column_name}")
    
    # Compare with historical baseline
    historical_mean = get_historical_mean(column_name)
    deviation_pct = abs((stats['mean'] - historical_mean) / historical_mean) * 100
    
    if deviation_pct > 20:  # 20% deviation threshold
        alert(f"{column_name} mean deviated {deviation_pct:.1f}% from baseline")
    
    return df
```

### 8. Completeness Checks

**Pattern**: Ensure all expected data is present.

**Example:**
```python
def check_completeness(df, date_column, expected_count=None):
    # Check record count
    actual_count = df.count()
    
    if expected_count and actual_count < expected_count:
        missing_pct = ((expected_count - actual_count) / expected_count) * 100
        alert(f"Missing {missing_pct:.1f}% of expected records")
    
    # Check for gaps in dates
    date_range = df.select(
        min(col(date_column)).alias("min_date"),
        max(col(date_column)).alias("max_date")
    ).collect()[0]
    
    expected_dates = pd.date_range(
        start=date_range['min_date'],
        end=date_range['max_date'],
        freq='D'
    )
    
    actual_dates = df.select(date_column).distinct().toPandas()[date_column]
    
    missing_dates = set(expected_dates) - set(actual_dates)
    
    if missing_dates:
        alert(f"Missing data for {len(missing_dates)} dates: {sorted(missing_dates)[:10]}")
    
    return df
```

### Data Quality Framework Implementation

**Example: Great Expectations**
```python
import great_expectations as ge

# Create expectation suite
df_ge = ge.from_pandas(df)

# Define expectations
df_ge.expect_column_values_to_not_be_null("user_id")
df_ge.expect_column_values_to_be_unique("user_id")
df_ge.expect_column_values_to_be_between("age", min_value=0, max_value=150)
df_ge.expect_column_values_to_be_in_set("country", ["US", "UK", "CA"])
df_ge.expect_column_values_to_match_regex("email", r"^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$")

# Validate
results = df_ge.validate()

if not results['success']:
    # Handle failures
    for result in results['results']:
        if not result['success']:
            print(f"Failed: {result['expectation_config']['expectation_type']}")
```

### Data Quality Metrics

**Track over time:**
```python
def calculate_quality_metrics(df):
    metrics = {
        'total_records': df.count(),
        'null_rate': df.select([
            (count(when(col(c).isNull(), c)) / count("*")).alias(c)
            for c in df.columns
        ]).collect()[0].asDict(),
        'duplicate_rate': (df.count() - df.distinct().count()) / df.count(),
        'schema_version': get_schema_version(df),
        'timestamp': datetime.now()
    }
    
    # Store metrics
    store_metrics(metrics)
    
    # Alert on degradation
    if metrics['duplicate_rate'] > 0.01:  # 1% threshold
        alert("High duplicate rate detected")
    
    return metrics
```

### Data Quality Checklist

✅ **Schema Validation**: Enforce data types  
✅ **Null Checks**: Required fields present  
✅ **Range Validation**: Values within bounds  
✅ **Uniqueness**: No duplicates  
✅ **Referential Integrity**: Foreign keys valid  
✅ **Freshness**: Data is recent  
✅ **Statistical Checks**: Detect anomalies  
✅ **Completeness**: All expected data present  
✅ **Format Validation**: Correct formats (email, phone)  
✅ **Business Rules**: Domain-specific validations  

---

## Real-World Architectures

### 1. E-Commerce Data Platform

```
┌─────────────────────────────────────────────────────────┐
│                    Data Sources                          │
├─────────────────────────────────────────────────────────┤
│  Web App  │  Mobile  │  Payment  │  Inventory  │  CRM   │
└────┬──────┴────┬─────┴────┬──────┴─────┬───────┴───┬────┘
     │           │          │            │           │
     └───────────┴──────────┴────────────┴───────────┘
                          │
                    ┌─────▼─────┐
                    │   Kafka   │
                    └─────┬─────┘
                          │
          ┌───────────────┼───────────────┐
          │               │               │
    ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
    │  Flink    │  │   Spark   │  │  Lambda   │
    │ (Stream)  │  │  (Batch)  │  │ Functions │
    └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
          │               │               │
          └───────────────┼───────────────┘
                          │
                    ┌─────▼─────┐
                    │    S3     │
                    │ Data Lake │
                    └─────┬─────┘
                          │
          ┌───────────────┼───────────────┐
          │               │               │
    ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
    │ Snowflake │  │ Redshift  │  │ BigQuery  │
    │Warehouse  │  │ Warehouse │  │ Warehouse │
    └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
          │               │               │
          └───────────────┼───────────────┘
                          │
                    ┌─────▼─────┐
                    │    BI     │
                    │  Tools    │
                    └───────────┘
```

**Key Components:**
- **Ingestion**: Kafka for event streaming
- **Stream Processing**: Flink for real-time
- **Batch Processing**: Spark for historical
- **Storage**: S3 data lake
- **Warehouse**: Snowflake/Redshift/BigQuery
- **Consumption**: BI tools, ML models

### 2. Real-Time Analytics Platform

```
Events → Kafka → Flink → Redis/Druid → Dashboard
                   ↓
                  S3 (Archive)
```

### 3. ML Feature Store

```
Raw Data → Feature Engineering → Feature Store → ML Models
                                      ↓
                                 Online Serving
                                 Offline Training
```

---

## Best Practices

### Design Principles

1. **Start Simple**: Begin with batch, add streaming if needed
2. **Idempotent by Default**: Design for retries
3. **Monitor Everything**: Metrics, logs, alerts
4. **Data Quality First**: Validate early and often
5. **Separate Concerns**: Decouple ingestion, processing, serving
6. **Version Everything**: Code, data, schemas
7. **Document Assumptions**: Data contracts, SLAs
8. **Plan for Failure**: Retries, DLQ, circuit breakers

### Operational Excellence

- **Monitoring**: Track latency, throughput, errors
- **Alerting**: Set up meaningful alerts
- **Logging**: Structured logs for debugging
- **Testing**: Unit, integration, end-to-end tests
- **Documentation**: Architecture diagrams, runbooks
- **Disaster Recovery**: Backup, restore procedures

---

## Interview Preparation

### Common Questions

**Q: Design a system to process 1 million events per second**
- Discuss: Kafka partitioning, Flink parallelism, scaling strategies

**Q: How do you ensure exactly-once processing?**
- Discuss: Idempotency, transactional writes, checkpointing

**Q: Design a data pipeline for real-time recommendations**
- Discuss: Feature store, online/offline serving, A/B testing

**Q: How do you handle late-arriving data?**
- Discuss: Watermarks, allowed lateness, reprocessing

**Q: Design a data quality framework**
- Discuss: Schema validation, Great Expectations, monitoring

---

## Next Steps

1. Review all example files
2. Implement sample pipelines
3. Practice system design questions
4. Build a portfolio project
5. Study real-world architectures

---

**Remember**: Good system design balances simplicity, scalability, and reliability!!
