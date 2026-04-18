# Data Engineering System Design - Quick Reference

## Batch vs Streaming Decision Matrix

| Requirement | Batch | Streaming |
|-------------|-------|-----------|
| **Latency < 1 min** | ❌ | ✅ |
| **Latency > 1 hour** | ✅ | ❌ |
| **Complete dataset needed** | ✅ | ❌ |
| **Real-time alerts** | ❌ | ✅ |
| **Cost-sensitive** | ✅ | ❌ |
| **Simple operations** | ✅ | ❌ |
| **Event-driven** | ❌ | ✅ |

## Data Ingestion Patterns

### Full Load
```python
# Simple but inefficient
df = spark.read.table("source")
df.write.mode("overwrite").save("target")
```

### Incremental (Timestamp)
```python
last_ts = get_watermark()
df = spark.read.table("source").filter(f"updated_at > '{last_ts}'")
df.write.mode("append").save("target")
```

### CDC (Change Data Capture)
```python
# Kafka + Debezium
cdc_stream = spark.readStream.format("kafka").load()
# Process INSERT, UPDATE, DELETE
```

### API Ingestion
```python
@retry_with_backoff(max_retries=3)
def fetch_api(url):
    response = requests.get(url)
    return response.json()
```

## Fault Tolerance Patterns

### Retry with Exponential Backoff
```python
@retry_with_exponential_backoff(max_retries=5, base_delay=1)
def risky_operation():
    # May fail, will retry automatically
    pass
```

### Circuit Breaker
```python
breaker = CircuitBreaker(failure_threshold=5, timeout=60)
result = breaker.call(external_service_call)
```

### Dead Letter Queue
```python
try:
    process(message)
except Exception as e:
    dlq.send_to_dlq(message, error=e, category='processing_errors')
```

### Checkpointing (Spark)
```python
query = df.writeStream \
    .option("checkpointLocation", "s3://checkpoints/") \
    .start()
```

## Idempotency Patterns

### Database Upsert
```sql
INSERT INTO table (id, name, value)
VALUES (1, 'Alice', 100)
ON CONFLICT (id) 
DO UPDATE SET name = EXCLUDED.name, value = EXCLUDED.value;
```

### Idempotency Keys
```python
def process_payment(data, idempotency_key):
    if idempotency_key in processed:
        return processed[idempotency_key]  # Return cached
    
    result = charge_payment(data)
    processed[idempotency_key] = result
    return result
```

### Deduplication
```python
# Spark deduplication
window = Window.partitionBy("id").orderBy(desc("timestamp"))
deduped = df.withColumn("rn", row_number().over(window)) \
    .filter("rn = 1").drop("rn")
```

### State Machine
```python
def transition(order_id, new_status):
    current = get_status(order_id)
    if current == new_status:
        return  # Already in desired state (idempotent)
    
    if new_status not in valid_transitions[current]:
        raise InvalidTransition()
    
    update_status(order_id, new_status)
```

## Data Quality Checks

### Schema Validation
```python
from pydantic import BaseModel, validator

class Event(BaseModel):
    user_id: str
    age: conint(ge=0, le=150)
    
    @validator('age')
    def age_valid(cls, v):
        if v < 0 or v > 150:
            raise ValueError('Invalid age')
        return v
```

### Null Checks
```python
null_pct = df.filter(col("user_id").isNull()).count() / df.count()
if null_pct > 0.05:  # 5% threshold
    raise DataQualityError("Too many nulls")
```

### Range Validation
```python
df = df.filter(
    (col("age") >= 0) & (col("age") <= 150) &
    (col("price") >= 0) & (col("price") <= 1000000)
)
```

### Uniqueness
```python
duplicates = df.groupBy("id").count().filter("count > 1")
if duplicates.count() > 0:
    # Handle duplicates
```

### Freshness
```python
latest = df.agg(max("timestamp")).collect()[0][0]
age_hours = (datetime.now() - latest).total_seconds() / 3600
if age_hours > 24:
    raise DataFreshnessError(f"Data {age_hours}h old")
```

## Common Architectures

### Lambda Architecture
```
Sources → Batch Layer (accurate) ─┐
       → Speed Layer (real-time) ─┤→ Serving Layer → Users
```

### Kappa Architecture
```
Sources → Stream Processing → Serving Layer → Users
```

### Medallion Architecture
```
Raw (Bronze) → Cleaned (Silver) → Aggregated (Gold)
```

## Performance Optimization

### Partition Pruning
```python
# Good: Uses partition pruning
df.filter("date = '2024-01-15'")

# Bad: Scans all partitions
df.filter("year(date) = 2024")
```

### Broadcast Joins
```python
# Small table broadcast
from pyspark.sql.functions import broadcast
df.join(broadcast(small_df), "key")
```

### Caching
```python
# Cache frequently accessed data
df.cache()
# or
df.persist(StorageLevel.MEMORY_AND_DISK)
```

## Monitoring Metrics

### Pipeline Health
- **Latency**: Time from event to processing
- **Throughput**: Records/second processed
- **Error Rate**: Failed records / total records
- **Data Freshness**: Age of latest data

### Data Quality Metrics
- **Null Rate**: % of null values
- **Duplicate Rate**: % of duplicate records
- **Schema Violations**: Records failing schema
- **Completeness**: % of expected data present

## Common Mistakes to Avoid

❌ **Non-idempotent operations**
```python
# Bad: Creates duplicates on retry
db.insert(record)

# Good: Idempotent upsert
db.upsert(record)
```

❌ **No retry logic**
```python
# Bad: Fails permanently on transient error
result = api.call()

# Good: Retries with backoff
@retry_with_backoff
def call_api():
    return api.call()
```

❌ **Ignoring data quality**
```python
# Bad: Process without validation
df.write.save("output")

# Good: Validate first
validated = validate_schema(df)
validated.write.save("output")
```

❌ **No monitoring**
```python
# Bad: Silent failures
process_data()

# Good: Log and alert
try:
    process_data()
    log_success_metric()
except Exception as e:
    log_error_metric(e)
    alert_on_call()
```

## Interview Questions Quick Answers

**Q: How do you ensure exactly-once processing?**
- Idempotent operations
- Transactional writes
- Offset tracking with processing
- Deduplication

**Q: Batch vs Streaming?**
- Batch: > 1hr latency, complete datasets, cost-effective
- Streaming: < 1min latency, real-time, event-driven

**Q: How to handle late data?**
- Watermarks in streaming
- Allowed lateness windows
- Reprocessing strategy

**Q: Data quality strategy?**
- Schema validation
- Null/range/uniqueness checks
- Referential integrity
- Freshness monitoring
- Statistical anomaly detection

**Q: Fault tolerance?**
- Retry with exponential backoff
- Circuit breakers
- Dead letter queues
- Checkpointing
- Graceful degradation

## Quick Commands

### Spark Streaming
```python
# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("subscribe", "topic") \
    .load()

# Write with checkpoint
query = df.writeStream \
    .option("checkpointLocation", "s3://checkpoints/") \
    .start()
```

### Delta Lake Operations
```python
# Merge (upsert)
deltaTable.merge(updates, "target.id = source.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Time travel
df = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("path")
```

### Data Quality
```python
# Great Expectations
df_ge = ge.from_pandas(df)
df_ge.expect_column_values_to_not_be_null("id")
df_ge.expect_column_values_to_be_unique("id")
results = df_ge.validate()
```

## Design Checklist

✅ **Scalability**
- Horizontal scaling capability
- Partition strategy defined
- Resource allocation planned

✅ **Reliability**
- Retry logic implemented
- Fault tolerance mechanisms
- Monitoring and alerting

✅ **Data Quality**
- Schema validation
- Quality checks at each stage
- DLQ for bad data

✅ **Idempotency**
- Operations safe to retry
- Deduplication strategy
- State management

✅ **Performance**
- Partition pruning enabled
- Appropriate caching
- Optimized joins

✅ **Observability**
- Logging structured
- Metrics tracked
- Alerts configured

## Resources

- **Books**: Designing Data-Intensive Applications (Kleppmann)
- **Courses**: System Design Interview courses
- **Tools**: Spark, Kafka, Airflow, dbt, Great Expectations
- **Blogs**: Netflix Tech Blog, Uber Engineering, Airbnb Engineering
