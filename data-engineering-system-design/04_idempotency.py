"""
Idempotency Patterns - Production Implementations
=================================================
This file demonstrates idempotency patterns to ensure operations
can be safely retried without side effects.
"""

import uuid
from datetime import datetime
from typing import Dict, Any, Optional
import hashlib
import json

# ============================================================================
# PATTERN 1: UPSERT OPERATIONS
# ============================================================================

def idempotent_database_upsert():
    """
    Use UPSERT (INSERT ... ON CONFLICT) for idempotent writes.
    Running multiple times produces same result.
    """
    import psycopg2
    
    conn = psycopg2.connect("postgresql://localhost/mydb")
    cursor = conn.cursor()
    
    customer_data = {
        'customer_id': 'CUST-123',
        'name': 'John Doe',
        'email': 'john@example.com',
        'updated_at': datetime.now()
    }
    
    # Idempotent upsert - safe to run multiple times
    cursor.execute("""
        INSERT INTO customers (customer_id, name, email, updated_at)
        VALUES (%(customer_id)s, %(name)s, %(email)s, %(updated_at)s)
        ON CONFLICT (customer_id) 
        DO UPDATE SET 
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            updated_at = EXCLUDED.updated_at
    """, customer_data)
    
    conn.commit()
    cursor.close()
    conn.close()


def spark_idempotent_merge():
    """
    Use Delta Lake MERGE for idempotent writes in Spark.
    """
    from pyspark.sql import SparkSession
    from delta.tables import DeltaTable
    
    spark = SparkSession.builder \
        .appName("IdempotentMerge") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # New data to merge
    updates_df = spark.read.parquet("s3://incoming/customer_updates/")
    
    # Target Delta table
    target_path = "s3://data-lake/customers/"
    
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        
        # Idempotent merge operation
        delta_table.alias("target").merge(
            updates_df.alias("source"),
            "target.customer_id = source.customer_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        # First run - create table
        updates_df.write.format("delta").save(target_path)
    
    spark.stop()


# ============================================================================
# PATTERN 2: IDEMPOTENCY KEYS
# ============================================================================

class IdempotentPaymentProcessor:
    """
    Payment processor using idempotency keys to prevent duplicate charges.
    """
    
    def __init__(self):
        self.processed_payments = {}  # In production: use database
    
    def process_payment(
        self,
        payment_data: Dict[str, Any],
        idempotency_key: str
    ) -> Dict[str, Any]:
        """
        Process payment with idempotency key.
        Multiple calls with same key return same result.
        """
        # Check if already processed
        if idempotency_key in self.processed_payments:
            print(f"Payment already processed with key: {idempotency_key}")
            return self.processed_payments[idempotency_key]
        
        # Process payment (expensive operation)
        result = self._charge_payment(payment_data)
        
        # Store result with idempotency key
        self.processed_payments[idempotency_key] = {
            'idempotency_key': idempotency_key,
            'payment_id': result['payment_id'],
            'amount': payment_data['amount'],
            'status': 'completed',
            'timestamp': datetime.now().isoformat()
        }
        
        return self.processed_payments[idempotency_key]
    
    def _charge_payment(self, payment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate payment gateway call."""
        return {
            'payment_id': str(uuid.uuid4()),
            'status': 'success'
        }


# Example usage
processor = IdempotentPaymentProcessor()

payment_data = {'amount': 100.00, 'currency': 'USD'}
idempotency_key = str(uuid.uuid4())

# First call - processes payment
result1 = processor.process_payment(payment_data, idempotency_key)

# Retry with same key - returns cached result (no duplicate charge)
result2 = processor.process_payment(payment_data, idempotency_key)

assert result1 == result2  # Same result


# ============================================================================
# PATTERN 3: DETERMINISTIC PROCESSING
# ============================================================================

def deterministic_transformation(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministic transformation - same input always produces same output.
    No random values, timestamps, or external dependencies.
    """
    # ✅ GOOD: Deterministic operations
    result = {
        'user_id': input_data['id'],
        'full_name': f"{input_data['first_name']} {input_data['last_name']}",
        'email_domain': input_data['email'].split('@')[1],
        'age': datetime.now().year - input_data['birth_year'],  # Deterministic for same day
        'account_type': 'premium' if input_data.get('is_premium') else 'basic'
    }
    
    return result


def non_deterministic_transformation_bad(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    ❌ BAD: Non-deterministic transformation.
    Different results on each run.
    """
    import random
    
    result = {
        'user_id': input_data['id'],
        'processed_at': datetime.now(),  # ❌ Changes each run
        'random_id': uuid.uuid4(),  # ❌ Changes each run
        'random_score': random.random()  # ❌ Changes each run
    }
    
    return result


# ============================================================================
# PATTERN 4: DEDUPLICATION
# ============================================================================

def deduplicate_events_spark():
    """
    Deduplicate events before processing to ensure idempotency.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import row_number, desc
    from pyspark.sql.window import Window
    
    spark = SparkSession.builder \
        .appName("Deduplication") \
        .getOrCreate()
    
    # Read events (may contain duplicates from retries)
    events = spark.read.parquet("s3://raw/events/")
    
    # Deduplicate by event_id, keeping latest by timestamp
    window = Window.partitionBy("event_id").orderBy(desc("timestamp"))
    
    deduped = events \
        .withColumn("row_num", row_number().over(window)) \
        .filter("row_num = 1") \
        .drop("row_num")
    
    # Write deduplicated data (idempotent)
    deduped.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet("s3://processed/events/")
    
    spark.stop()


def deduplicate_with_hash():
    """
    Use content hash for deduplication.
    """
    def calculate_content_hash(record: Dict[str, Any]) -> str:
        """Calculate deterministic hash of record content."""
        # Sort keys for deterministic hash
        sorted_record = json.dumps(record, sort_keys=True)
        return hashlib.sha256(sorted_record.encode()).hexdigest()
    
    seen_hashes = set()
    unique_records = []
    
    records = [
        {'id': '1', 'name': 'Alice', 'value': 100},
        {'id': '2', 'name': 'Bob', 'value': 200},
        {'id': '1', 'name': 'Alice', 'value': 100},  # Duplicate
    ]
    
    for record in records:
        content_hash = calculate_content_hash(record)
        
        if content_hash not in seen_hashes:
            seen_hashes.add(content_hash)
            unique_records.append(record)
    
    print(f"Original: {len(records)}, Unique: {len(unique_records)}")
    return unique_records


# ============================================================================
# PATTERN 5: STATE MACHINE FOR IDEMPOTENT TRANSITIONS
# ============================================================================

class OrderStateMachine:
    """
    State machine ensuring idempotent state transitions.
    """
    
    VALID_TRANSITIONS = {
        'pending': ['confirmed', 'cancelled'],
        'confirmed': ['shipped', 'cancelled'],
        'shipped': ['delivered', 'returned'],
        'delivered': ['returned'],
        'cancelled': [],
        'returned': []
    }
    
    def __init__(self):
        self.orders = {}  # In production: use database
    
    def transition_order(
        self,
        order_id: str,
        new_status: str,
        idempotency_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Idempotent state transition.
        """
        # Get current order state
        order = self.orders.get(order_id, {'status': 'pending'})
        current_status = order['status']
        
        # Check if already in desired state (idempotent)
        if current_status == new_status:
            print(f"Order {order_id} already in {new_status} state")
            return order
        
        # Check if transition is valid
        valid_next_states = self.VALID_TRANSITIONS.get(current_status, [])
        
        if new_status not in valid_next_states:
            raise ValueError(
                f"Invalid transition from {current_status} to {new_status}. "
                f"Valid transitions: {valid_next_states}"
            )
        
        # Check idempotency key if provided
        if idempotency_key:
            last_transition_key = order.get('last_transition_key')
            if last_transition_key == idempotency_key:
                print(f"Transition already applied with key: {idempotency_key}")
                return order
        
        # Apply transition
        order['status'] = new_status
        order['updated_at'] = datetime.now().isoformat()
        order['last_transition_key'] = idempotency_key
        
        self.orders[order_id] = order
        
        print(f"Order {order_id} transitioned: {current_status} -> {new_status}")
        return order


# Example usage
state_machine = OrderStateMachine()

# Transition order
state_machine.transition_order('ORDER-123', 'confirmed', idempotency_key='key-1')

# Retry with same key - idempotent (no error)
state_machine.transition_order('ORDER-123', 'confirmed', idempotency_key='key-1')

# Try to transition to same state - idempotent
state_machine.transition_order('ORDER-123', 'confirmed')


# ============================================================================
# PATTERN 6: EXACTLY-ONCE SEMANTICS WITH TRANSACTIONS
# ============================================================================

def exactly_once_kafka_to_db():
    """
    Achieve exactly-once semantics by combining Kafka offsets with database transactions.
    """
    from kafka import KafkaConsumer
    import psycopg2
    
    consumer = KafkaConsumer(
        'events',
        bootstrap_servers=['localhost:9092'],
        enable_auto_commit=False,
        group_id='exactly-once-processor'
    )
    
    conn = psycopg2.connect("postgresql://localhost/mydb")
    
    for message in consumer:
        cursor = conn.cursor()
        
        try:
            # Start transaction
            cursor.execute("BEGIN")
            
            # Check if offset already processed
            cursor.execute("""
                SELECT 1 FROM processed_offsets 
                WHERE topic = %s AND partition = %s AND offset = %s
            """, (message.topic, message.partition, message.offset))
            
            if cursor.fetchone():
                # Already processed - skip (idempotent)
                cursor.execute("COMMIT")
                consumer.commit()
                continue
            
            # Process message (idempotent operation)
            event_data = json.loads(message.value)
            
            cursor.execute("""
                INSERT INTO events (event_id, data, processed_at)
                VALUES (%(event_id)s, %(data)s, NOW())
                ON CONFLICT (event_id) DO NOTHING
            """, event_data)
            
            # Record processed offset
            cursor.execute("""
                INSERT INTO processed_offsets (topic, partition, offset, processed_at)
                VALUES (%s, %s, %s, NOW())
            """, (message.topic, message.partition, message.offset))
            
            # Commit transaction and offset together
            cursor.execute("COMMIT")
            consumer.commit()
            
        except Exception as e:
            cursor.execute("ROLLBACK")
            print(f"Error processing message: {e}")
            raise
        
        finally:
            cursor.close()
    
    conn.close()


# ============================================================================
# PATTERN 7: VERSIONING FOR IDEMPOTENCY
# ============================================================================

class VersionedRecord:
    """
    Use version numbers to ensure idempotent updates.
    """
    
    def __init__(self):
        self.records = {}
    
    def update_record(
        self,
        record_id: str,
        new_data: Dict[str, Any],
        expected_version: int
    ) -> Dict[str, Any]:
        """
        Update record only if version matches (optimistic locking).
        Idempotent - retrying with same version is safe.
        """
        current_record = self.records.get(record_id, {
            'version': 0,
            'data': {}
        })
        
        current_version = current_record['version']
        
        # Check version
        if current_version > expected_version:
            # Already updated by another process
            print(f"Record {record_id} already at version {current_version}")
            return current_record
        
        elif current_version < expected_version:
            # Version mismatch - conflict
            raise ValueError(
                f"Version conflict. Expected {expected_version}, "
                f"but current is {current_version}"
            )
        
        # Version matches - apply update
        updated_record = {
            'version': current_version + 1,
            'data': new_data,
            'updated_at': datetime.now().isoformat()
        }
        
        self.records[record_id] = updated_record
        
        print(f"Updated record {record_id} to version {updated_record['version']}")
        return updated_record


# Example usage
versioned = VersionedRecord()

# Update record
versioned.update_record('REC-1', {'name': 'Alice'}, expected_version=0)

# Retry with same version - idempotent (returns already updated record)
versioned.update_record('REC-1', {'name': 'Alice'}, expected_version=0)


# ============================================================================
# PATTERN 8: IDEMPOTENT FILE PROCESSING
# ============================================================================

class IdempotentFileProcessor:
    """
    Process files idempotently by tracking processed files.
    """
    
    def __init__(self, metadata_store: str):
        self.metadata_store = metadata_store
        self.processed_files = self._load_processed_files()
    
    def _load_processed_files(self) -> set:
        """Load list of already processed files."""
        # In production: read from database or metadata file
        return set()
    
    def _mark_file_processed(self, file_path: str, file_hash: str):
        """Mark file as processed."""
        self.processed_files.add((file_path, file_hash))
        # In production: persist to database
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate file content hash."""
        import hashlib
        
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hasher.update(chunk)
        
        return hasher.hexdigest()
    
    def process_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process file idempotently.
        Same file content processed multiple times has no effect.
        """
        # Calculate file hash
        file_hash = self._calculate_file_hash(file_path)
        
        # Check if already processed
        if (file_path, file_hash) in self.processed_files:
            print(f"File already processed: {file_path} (hash: {file_hash[:8]}...)")
            return {'status': 'already_processed', 'file': file_path}
        
        # Process file
        result = self._process_file_content(file_path)
        
        # Mark as processed
        self._mark_file_processed(file_path, file_hash)
        
        return {
            'status': 'processed',
            'file': file_path,
            'hash': file_hash,
            'result': result
        }
    
    def _process_file_content(self, file_path: str) -> Dict[str, Any]:
        """Process file content."""
        # Simulate processing
        return {'records_processed': 100}


# ============================================================================
# IDEMPOTENCY TESTING
# ============================================================================

def test_idempotency(func, *args, **kwargs):
    """
    Test if a function is idempotent by running it multiple times.
    """
    print(f"Testing idempotency of {func.__name__}")
    
    # Run function multiple times
    results = []
    for i in range(3):
        result = func(*args, **kwargs)
        results.append(result)
        print(f"Run {i+1}: {result}")
    
    # Check if all results are identical
    if all(r == results[0] for r in results):
        print("✅ Function is idempotent")
        return True
    else:
        print("❌ Function is NOT idempotent")
        return False


# ============================================================================
# IDEMPOTENCY CHECKLIST
# ============================================================================

"""
IDEMPOTENCY IMPLEMENTATION CHECKLIST:

✅ Database Operations
   - Use UPSERT (INSERT ... ON CONFLICT)
   - Use MERGE for Delta/Iceberg tables
   - Unique constraints on natural keys

✅ Idempotency Keys
   - Client-generated unique IDs
   - Store results with keys
   - Return cached results for duplicate keys

✅ Deterministic Processing
   - No random values
   - No timestamps (unless part of input)
   - No external state dependencies

✅ Deduplication
   - Remove duplicates before processing
   - Use content hashing
   - Window functions for latest record

✅ State Machines
   - Check current state before transition
   - Validate transitions
   - Idempotent state checks

✅ Transactions
   - Atomic operations
   - Combine offset tracking with processing
   - Rollback on failure

✅ Versioning
   - Optimistic locking
   - Version checks before updates
   - Handle version conflicts

✅ File Processing
   - Track processed files
   - Use content hashing
   - Skip already processed files

TESTING:
- Run operations multiple times
- Verify same results
- Check for side effects
- Test with retries
"""

if __name__ == "__main__":
    print("Idempotency Patterns Examples")
    print("Run individual functions to see implementations")
    
    # Test idempotency
    # test_idempotency(deterministic_transformation, {'id': '1', 'first_name': 'John', 
    #                  'last_name': 'Doe', 'email': 'john@example.com', 
    #                  'birth_year': 1990, 'is_premium': True})
