"""
Data Ingestion Patterns - Practical Implementations
===================================================
This file demonstrates various data ingestion patterns used in production systems.
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ============================================================================
# PATTERN 1: FULL LOAD (SNAPSHOT)
# ============================================================================

def full_load_pattern():
    """
    Load complete dataset each time.
    Simple but inefficient for large tables.
    """
    spark = SparkSession.builder \
        .appName("FullLoad") \
        .getOrCreate()
    
    print("Starting full load...")
    
    # Extract: Read entire source table
    source_data = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://source-db:5432/production") \
        .option("dbtable", "customers") \
        .option("user", "readonly_user") \
        .option("password", "password") \
        .load()
    
    print(f"Extracted {source_data.count()} records from source")
    
    # Transform: Add metadata
    transformed = source_data.withColumn("load_timestamp", current_timestamp()) \
        .withColumn("load_date", current_date())
    
    # Load: Overwrite target
    transformed.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .parquet("s3://data-lake/customers/")
    
    print("Full load completed")
    
    spark.stop()


# ============================================================================
# PATTERN 2: INCREMENTAL LOAD - TIMESTAMP BASED
# ============================================================================

def incremental_load_timestamp():
    """
    Load only records modified since last run.
    Uses timestamp column for tracking changes.
    """
    spark = SparkSession.builder \
        .appName("IncrementalLoad") \
        .getOrCreate()
    
    # Get last processed timestamp from metadata table
    last_watermark = get_last_watermark("orders")
    
    if last_watermark is None:
        # First run - load all data
        last_watermark = "1970-01-01 00:00:00"
    
    print(f"Loading data modified after {last_watermark}")
    
    # Extract: Only new/updated records
    incremental_data = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://source-db:5432/production") \
        .option("dbtable", f"""
            (SELECT * FROM orders 
             WHERE updated_at > '{last_watermark}'
             ORDER BY updated_at) as incremental
        """) \
        .option("user", "readonly_user") \
        .option("password", "password") \
        .load()
    
    record_count = incremental_data.count()
    print(f"Extracted {record_count} new/updated records")
    
    if record_count > 0:
        # Get max timestamp for next run
        max_timestamp = incremental_data.agg(max("updated_at")).collect()[0][0]
        
        # Transform
        transformed = incremental_data.withColumn("ingestion_timestamp", current_timestamp())
        
        # Load: Append to target
        transformed.write \
            .mode("append") \
            .partitionBy("order_date") \
            .parquet("s3://data-lake/orders/")
        
        # Update watermark
        update_watermark("orders", max_timestamp)
        
        print(f"Incremental load completed. Next watermark: {max_timestamp}")
    else:
        print("No new records to process")
    
    spark.stop()


def get_last_watermark(table_name: str) -> str:
    """Retrieve last processed timestamp from metadata store."""
    # In production, read from metadata database
    # For demo, return mock value
    return "2024-01-01 00:00:00"


def update_watermark(table_name: str, timestamp: str):
    """Update watermark in metadata store."""
    # In production, write to metadata database
    print(f"Updated watermark for {table_name}: {timestamp}")


# ============================================================================
# PATTERN 3: CHANGE DATA CAPTURE (CDC)
# ============================================================================

def cdc_ingestion_debezium():
    """
    Capture database changes using CDC (Debezium).
    Processes INSERT, UPDATE, DELETE operations.
    """
    spark = SparkSession.builder \
        .appName("CDCIngestion") \
        .getOrCreate()
    
    # Read CDC events from Kafka (produced by Debezium)
    cdc_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "dbserver1.inventory.customers") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse Debezium CDC format
    from pyspark.sql.types import StructType, StructField, StringType, StructType as ST
    
    # Debezium envelope schema
    schema = StructType([
        StructField("before", ST([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType())
        ])),
        StructField("after", ST([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType())
        ])),
        StructField("op", StringType()),  # c=create, u=update, d=delete
        StructField("ts_ms", StringType())
    ])
    
    parsed_cdc = cdc_stream.select(
        from_json(col("value").cast("string"), schema).alias("payload")
    ).select("payload.*")
    
    # Process different operation types
    def process_cdc_batch(df, epoch_id):
        """Process CDC events by operation type."""
        
        # Inserts (op='c') and Updates (op='u')
        upserts = df.filter(col("op").isin(["c", "u"])) \
            .select(col("after.*"))
        
        if upserts.count() > 0:
            # Write to Delta Lake for ACID transactions
            upserts.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save("s3://data-lake/customers_delta/")
        
        # Deletes (op='d')
        deletes = df.filter(col("op") == "d") \
            .select(col("before.id").alias("id"))
        
        if deletes.count() > 0:
            # Mark as deleted or remove from target
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forPath(spark, "s3://data-lake/customers_delta/")
            delta_table.delete(
                condition=col("id").isin([row.id for row in deletes.collect()])
            )
    
    # Start streaming query
    query = parsed_cdc.writeStream \
        .foreachBatch(process_cdc_batch) \
        .option("checkpointLocation", "s3://checkpoints/cdc_customers/") \
        .start()
    
    query.awaitTermination()


# ============================================================================
# PATTERN 4: API-BASED INGESTION
# ============================================================================

class APIIngestion:
    """
    Ingest data from REST APIs with pagination, rate limiting, and retry logic.
    """
    
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })
    
    def fetch_with_pagination(self, endpoint: str, params: Dict = None) -> List[Dict]:
        """
        Fetch data with automatic pagination handling.
        """
        all_data = []
        page = 1
        
        while True:
            # Add pagination parameters
            page_params = params.copy() if params else {}
            page_params['page'] = page
            page_params['per_page'] = 100
            
            # Make request with retry logic
            response = self._make_request_with_retry(endpoint, page_params)
            
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                break
            
            data = response.json()
            
            # Check if we have data
            if not data.get('results'):
                break
            
            all_data.extend(data['results'])
            
            # Check if there are more pages
            if not data.get('has_next_page'):
                break
            
            page += 1
            
            # Rate limiting - wait between requests
            time.sleep(0.5)
        
        print(f"Fetched {len(all_data)} records from {endpoint}")
        return all_data
    
    def _make_request_with_retry(self, endpoint: str, params: Dict, max_retries: int = 3):
        """
        Make HTTP request with exponential backoff retry.
        """
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    print(f"Request failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise
        
        raise Exception(f"Failed after {max_retries} retries")
    
    def incremental_fetch(self, endpoint: str, since: datetime) -> List[Dict]:
        """
        Fetch only data updated since a specific timestamp.
        """
        params = {
            'updated_since': since.isoformat(),
            'sort': 'updated_at',
            'order': 'asc'
        }
        
        return self.fetch_with_pagination(endpoint, params)


def api_ingestion_example():
    """
    Example: Ingest customer data from API.
    """
    # Initialize API client
    api = APIIngestion(
        base_url="https://api.example.com/v1",
        api_key="your_api_key_here"
    )
    
    # Get last sync timestamp
    last_sync = datetime.now() - timedelta(days=1)
    
    # Fetch incremental data
    customers = api.incremental_fetch("customers", since=last_sync)
    
    # Convert to DataFrame
    df = pd.DataFrame(customers)
    
    # Write to storage
    df.to_parquet('s3://raw-data/api_customers/latest.parquet')
    
    print(f"Ingested {len(customers)} customers from API")


# ============================================================================
# PATTERN 5: FILE-BASED INGESTION
# ============================================================================

def file_based_ingestion_autoloader():
    """
    Ingest files as they arrive in cloud storage.
    Uses Databricks Auto Loader (cloudFiles) or similar.
    """
    spark = SparkSession.builder \
        .appName("FileIngestion") \
        .getOrCreate()
    
    # Monitor directory for new files
    incoming_files = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "s3://schemas/incoming/") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
        .load("s3://incoming/raw-files/")
    
    # Add metadata
    processed = incoming_files.withColumn("ingestion_time", current_timestamp()) \
        .withColumn("file_name", input_file_name())
    
    # Write to processed zone
    query = processed.writeStream \
        .format("delta") \
        .option("checkpointLocation", "s3://checkpoints/file_ingestion/") \
        .option("mergeSchema", "true") \
        .start("s3://processed/files/")
    
    query.awaitTermination()


def batch_file_ingestion():
    """
    Batch process files from a directory.
    Tracks processed files to avoid reprocessing.
    """
    spark = SparkSession.builder \
        .appName("BatchFileIngestion") \
        .getOrCreate()
    
    # List files in directory
    import boto3
    s3 = boto3.client('s3')
    
    bucket = 'incoming-data'
    prefix = 'raw-files/'
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    # Get list of processed files
    processed_files = get_processed_files()
    
    for obj in response.get('Contents', []):
        file_key = obj['Key']
        
        # Skip if already processed
        if file_key in processed_files:
            continue
        
        # Process file
        try:
            file_path = f"s3://{bucket}/{file_key}"
            
            # Read file
            df = spark.read.json(file_path)
            
            # Add metadata
            df = df.withColumn("source_file", lit(file_key)) \
                .withColumn("ingestion_time", current_timestamp())
            
            # Write to processed zone
            df.write \
                .mode("append") \
                .partitionBy("ingestion_date") \
                .parquet("s3://processed/data/")
            
            # Mark as processed
            mark_file_processed(file_key)
            
            # Optionally move to archive
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': file_key},
                Key=f"archive/{file_key}"
            )
            s3.delete_object(Bucket=bucket, Key=file_key)
            
            print(f"Processed: {file_key}")
            
        except Exception as e:
            print(f"Error processing {file_key}: {e}")
            # Move to error folder
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': file_key},
                Key=f"errors/{file_key}"
            )
    
    spark.stop()


def get_processed_files() -> set:
    """Get list of already processed files."""
    # In production, query metadata database
    return set()


def mark_file_processed(file_key: str):
    """Mark file as processed in metadata store."""
    # In production, insert into metadata database
    print(f"Marked as processed: {file_key}")


# ============================================================================
# PATTERN 6: STREAMING INGESTION (KAFKA)
# ============================================================================

def kafka_producer_example():
    """
    Produce events to Kafka topic.
    """
    from kafka import KafkaProducer
    import json
    
    # Initialize producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',  # Wait for all replicas
        retries=3,
        max_in_flight_requests_per_connection=1  # Ensure ordering
    )
    
    # Send events
    events = [
        {'user_id': '123', 'action': 'login', 'timestamp': datetime.now().isoformat()},
        {'user_id': '456', 'action': 'purchase', 'timestamp': datetime.now().isoformat()},
    ]
    
    for event in events:
        # Use user_id as key for partitioning
        future = producer.send(
            topic='user_events',
            key=event['user_id'],
            value=event
        )
        
        # Wait for confirmation
        try:
            record_metadata = future.get(timeout=10)
            print(f"Sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
        except Exception as e:
            print(f"Failed to send event: {e}")
    
    producer.flush()
    producer.close()


def kafka_consumer_example():
    """
    Consume events from Kafka and write to storage.
    """
    spark = SparkSession.builder \
        .appName("KafkaConsumer") \
        .getOrCreate()
    
    # Read from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user_events") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .load()
    
    # Parse JSON messages
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("action", StringType()),
        StructField("timestamp", TimestampType())
    ])
    
    parsed = kafka_stream.select(
        col("key").cast("string").alias("key"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp")
    ).select("key", "data.*", "topic", "partition", "offset", "kafka_timestamp")
    
    # Write to storage with exactly-once semantics
    query = parsed.writeStream \
        .format("delta") \
        .option("checkpointLocation", "s3://checkpoints/kafka_consumer/") \
        .outputMode("append") \
        .start("s3://data-lake/user_events/")
    
    query.awaitTermination()


# ============================================================================
# PATTERN 7: DATABASE REPLICATION
# ============================================================================

def database_replication_dms():
    """
    Configure AWS DMS for database replication.
    This is typically done via AWS Console or CloudFormation.
    """
    import boto3
    
    dms = boto3.client('dms')
    
    # Create replication instance
    replication_instance = dms.create_replication_instance(
        ReplicationInstanceIdentifier='my-replication-instance',
        ReplicationInstanceClass='dms.t3.medium',
        AllocatedStorage=100,
        VpcSecurityGroupIds=['sg-12345678'],
        AvailabilityZone='us-east-1a',
        PubliclyAccessible=False
    )
    
    # Create source endpoint (PostgreSQL)
    source_endpoint = dms.create_endpoint(
        EndpointIdentifier='source-postgres',
        EndpointType='source',
        EngineName='postgres',
        ServerName='source-db.example.com',
        Port=5432,
        DatabaseName='production',
        Username='replication_user',
        Password='password'
    )
    
    # Create target endpoint (S3)
    target_endpoint = dms.create_endpoint(
        EndpointIdentifier='target-s3',
        EndpointType='target',
        EngineName='s3',
        S3Settings={
            'BucketName': 'data-lake',
            'BucketFolder': 'replicated-data',
            'DataFormat': 'parquet',
            'CompressionType': 'gzip'
        }
    )
    
    # Create replication task
    replication_task = dms.create_replication_task(
        ReplicationTaskIdentifier='replicate-production-db',
        SourceEndpointArn=source_endpoint['Endpoint']['EndpointArn'],
        TargetEndpointArn=target_endpoint['Endpoint']['EndpointArn'],
        ReplicationInstanceArn=replication_instance['ReplicationInstance']['ReplicationInstanceArn'],
        MigrationType='cdc',  # Change Data Capture
        TableMappings=json.dumps({
            "rules": [
                {
                    "rule-type": "selection",
                    "rule-id": "1",
                    "rule-name": "include-all-tables",
                    "object-locator": {
                        "schema-name": "public",
                        "table-name": "%"
                    },
                    "rule-action": "include"
                }
            ]
        })
    )
    
    print("DMS replication configured")


# ============================================================================
# PATTERN SELECTION GUIDE
# ============================================================================

"""
INGESTION PATTERN SELECTION:

1. FULL LOAD
   - Use when: Small tables, data changes frequently
   - Pros: Simple, always in sync
   - Cons: Inefficient for large tables
   - Example: Reference tables, configuration data

2. INCREMENTAL (TIMESTAMP)
   - Use when: Tables have updated_at column
   - Pros: Efficient, low source load
   - Cons: Requires timestamp column
   - Example: Transaction tables, user profiles

3. CDC (CHANGE DATA CAPTURE)
   - Use when: Need all changes (INSERT/UPDATE/DELETE)
   - Pros: Captures all changes, low latency
   - Cons: Complex setup, requires database permissions
   - Example: Critical transactional data

4. API-BASED
   - Use when: Data from external services
   - Pros: Standard interface, controlled access
   - Cons: Rate limits, pagination complexity
   - Example: SaaS platforms, third-party data

5. FILE-BASED
   - Use when: Data arrives as files
   - Pros: Simple, batch-friendly
   - Cons: Latency, file management
   - Example: Partner data feeds, exports

6. STREAMING (KAFKA)
   - Use when: Real-time event processing needed
   - Pros: Low latency, scalable
   - Cons: Complex, higher cost
   - Example: Clickstreams, IoT sensors

7. DATABASE REPLICATION
   - Use when: Entire database sync needed
   - Pros: Automated, reliable
   - Cons: Cost, vendor lock-in
   - Example: DR, analytics replica
"""

if __name__ == "__main__":
    print("Data Ingestion Patterns Examples")
    print("Run individual functions to see implementations")
