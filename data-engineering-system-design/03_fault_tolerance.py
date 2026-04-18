"""
Fault Tolerance Patterns - Production-Ready Implementations
===========================================================
This file demonstrates fault tolerance strategies for data engineering systems.
"""

import time
import random
from functools import wraps
from typing import Callable, Any, Optional
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# PATTERN 1: RETRY WITH EXPONENTIAL BACKOFF
# ============================================================================

def retry_with_exponential_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True
):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        exponential_base: Base for exponential calculation
        jitter: Add randomness to prevent thundering herd
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            retries = 0
            
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    retries += 1
                    
                    if retries > max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__}")
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = min(
                        base_delay * (exponential_base ** (retries - 1)),
                        max_delay
                    )
                    
                    # Add jitter to prevent thundering herd
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    
                    logger.warning(
                        f"Attempt {retries}/{max_retries} failed for {func.__name__}. "
                        f"Retrying in {delay:.2f}s. Error: {str(e)}"
                    )
                    
                    time.sleep(delay)
            
        return wrapper
    return decorator


# Example usage
@retry_with_exponential_backoff(max_retries=5, base_delay=1, max_delay=30)
def fetch_data_from_api(url: str):
    """Fetch data from API with automatic retry."""
    import requests
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


# ============================================================================
# PATTERN 2: CIRCUIT BREAKER
# ============================================================================

class CircuitBreaker:
    """
    Circuit breaker pattern to prevent cascading failures.
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failures exceeded threshold, requests fail fast
    - HALF_OPEN: Testing if service recovered
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker entering HALF_OPEN state")
            else:
                raise Exception(f"Circuit breaker is OPEN. Service unavailable.")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery."""
        return (
            self.last_failure_time is not None and
            time.time() - self.last_failure_time >= self.recovery_timeout
        )
    
    def _on_success(self):
        """Handle successful call."""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            self.failure_count = 0
            logger.info("Circuit breaker CLOSED - service recovered")
    
    def _on_failure(self):
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.error(
                f"Circuit breaker OPEN - failure threshold ({self.failure_threshold}) exceeded"
            )


# Example usage
breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

def call_external_service():
    """Call external service with circuit breaker protection."""
    return breaker.call(fetch_data_from_api, "https://api.example.com/data")


# ============================================================================
# PATTERN 3: DEAD LETTER QUEUE (DLQ)
# ============================================================================

class DeadLetterQueue:
    """
    Dead Letter Queue for handling failed messages.
    """
    
    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        self.dlq_categories = {
            'validation_errors': [],
            'processing_errors': [],
            'unknown_errors': []
        }
    
    def send_to_dlq(
        self,
        message: Any,
        error: Exception,
        category: str = 'unknown_errors',
        metadata: dict = None
    ):
        """Send failed message to DLQ with error details."""
        
        dlq_entry = {
            'message': message,
            'error': str(error),
            'error_type': type(error).__name__,
            'category': category,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        self.dlq_categories[category].append(dlq_entry)
        
        logger.warning(
            f"Message sent to DLQ ({category}): {error}"
        )
        
        # In production, write to persistent storage
        self._persist_to_storage(dlq_entry, category)
    
    def _persist_to_storage(self, entry: dict, category: str):
        """Persist DLQ entry to storage."""
        import json
        
        # Write to file (in production, use S3, database, etc.)
        filename = f"{self.storage_path}/{category}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Mock implementation
        logger.info(f"Would write to: {filename}")
    
    def get_dlq_messages(self, category: str = None):
        """Retrieve messages from DLQ for reprocessing."""
        if category:
            return self.dlq_categories.get(category, [])
        return self.dlq_categories
    
    def reprocess_dlq(self, category: str, processor: Callable):
        """Attempt to reprocess messages from DLQ."""
        messages = self.dlq_categories.get(category, [])
        
        reprocessed = 0
        failed = 0
        
        for entry in messages:
            try:
                processor(entry['message'])
                reprocessed += 1
            except Exception as e:
                failed += 1
                logger.error(f"Reprocessing failed: {e}")
        
        logger.info(
            f"DLQ reprocessing complete. "
            f"Success: {reprocessed}, Failed: {failed}"
        )


# Example usage
dlq = DeadLetterQueue(storage_path="s3://dlq/")

def process_message_with_dlq(message):
    """Process message with DLQ for failures."""
    try:
        # Validate message
        if not message.get('user_id'):
            raise ValueError("Missing user_id")
        
        # Process message
        result = process_data(message)
        return result
        
    except ValueError as e:
        # Validation errors
        dlq.send_to_dlq(message, e, category='validation_errors')
        
    except ConnectionError as e:
        # Transient errors - could retry
        dlq.send_to_dlq(message, e, category='processing_errors',
                       metadata={'retryable': True})
        
    except Exception as e:
        # Unknown errors
        dlq.send_to_dlq(message, e, category='unknown_errors')


def process_data(message):
    """Mock data processing function."""
    return {"status": "processed", "data": message}


# ============================================================================
# PATTERN 4: CHECKPOINTING (SPARK STREAMING)
# ============================================================================

def spark_streaming_with_checkpoint():
    """
    Spark Structured Streaming with checkpointing for fault tolerance.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    
    spark = SparkSession.builder \
        .appName("FaultTolerantStreaming") \
        .getOrCreate()
    
    # Read from Kafka
    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Process stream
    processed = stream.select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("timestamp")
    )
    
    # Write with checkpoint for fault tolerance
    query = processed.writeStream \
        .format("parquet") \
        .option("path", "s3://output/events/") \
        .option("checkpointLocation", "s3://checkpoints/events/") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # If job fails and restarts, it resumes from checkpoint
    query.awaitTermination()


# ============================================================================
# PATTERN 5: IDEMPOTENT WRITES WITH DEDUPLICATION
# ============================================================================

def idempotent_write_with_dedup():
    """
    Ensure idempotent writes by deduplicating before writing.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.window import Window
    
    spark = SparkSession.builder \
        .appName("IdempotentWrite") \
        .getOrCreate()
    
    # Read data (may contain duplicates due to retries)
    data = spark.read.parquet("s3://input/events/")
    
    # Deduplicate by event_id (keep latest by timestamp)
    window = Window.partitionBy("event_id").orderBy(desc("timestamp"))
    
    deduped = data.withColumn("row_num", row_number().over(window)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    # Write with overwrite mode (idempotent)
    deduped.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet("s3://output/events_deduped/")
    
    spark.stop()


# ============================================================================
# PATTERN 6: GRACEFUL DEGRADATION
# ============================================================================

class ServiceWithFallback:
    """
    Service with multiple fallback levels for graceful degradation.
    """
    
    def __init__(self):
        self.ml_model_available = True
        self.rule_engine_available = True
    
    def get_recommendations(self, user_id: str):
        """
        Get recommendations with fallback chain:
        1. ML Model (best quality)
        2. Rule-based engine (good quality)
        3. Popular items (basic quality)
        """
        try:
            # Try ML model first
            if self.ml_model_available:
                return self._ml_recommendations(user_id)
                
        except Exception as e:
            logger.warning(f"ML model failed: {e}. Falling back to rules.")
            self.ml_model_available = False
        
        try:
            # Fallback to rule-based
            if self.rule_engine_available:
                return self._rule_based_recommendations(user_id)
                
        except Exception as e:
            logger.warning(f"Rule engine failed: {e}. Falling back to popular items.")
            self.rule_engine_available = False
        
        # Final fallback - popular items
        return self._popular_items()
    
    def _ml_recommendations(self, user_id: str):
        """Get ML-based recommendations."""
        # Simulate ML model call
        return ["item1", "item2", "item3"]
    
    def _rule_based_recommendations(self, user_id: str):
        """Get rule-based recommendations."""
        # Simulate rule engine
        return ["item4", "item5", "item6"]
    
    def _popular_items(self):
        """Get popular items as last resort."""
        return ["popular1", "popular2", "popular3"]


# ============================================================================
# PATTERN 7: BULKHEAD PATTERN
# ============================================================================

from concurrent.futures import ThreadPoolExecutor
from queue import Queue

class BulkheadExecutor:
    """
    Isolate resources using bulkhead pattern to prevent total system failure.
    """
    
    def __init__(self):
        # Separate thread pools for different priorities
        self.critical_pool = ThreadPoolExecutor(max_workers=10, thread_name_prefix="critical")
        self.normal_pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="normal")
        self.low_priority_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="low")
    
    def submit_critical_task(self, func, *args, **kwargs):
        """Submit critical task with dedicated resources."""
        return self.critical_pool.submit(func, *args, **kwargs)
    
    def submit_normal_task(self, func, *args, **kwargs):
        """Submit normal priority task."""
        return self.normal_pool.submit(func, *args, **kwargs)
    
    def submit_low_priority_task(self, func, *args, **kwargs):
        """Submit low priority task."""
        return self.low_priority_pool.submit(func, *args, **kwargs)
    
    def shutdown(self):
        """Shutdown all thread pools."""
        self.critical_pool.shutdown(wait=True)
        self.normal_pool.shutdown(wait=True)
        self.low_priority_pool.shutdown(wait=True)


# Example usage
executor = BulkheadExecutor()

# Critical tasks get dedicated resources
critical_future = executor.submit_critical_task(process_payment, payment_data)

# Low priority tasks won't affect critical tasks
low_priority_future = executor.submit_low_priority_task(generate_report, report_params)


# ============================================================================
# PATTERN 8: TIMEOUT PROTECTION
# ============================================================================

import signal
from contextlib import contextmanager

class TimeoutError(Exception):
    pass

@contextmanager
def timeout(seconds: int):
    """
    Context manager for timeout protection.
    """
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Operation timed out after {seconds} seconds")
    
    # Set the signal handler
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


# Example usage
def query_with_timeout(query: str, timeout_seconds: int = 30):
    """Execute database query with timeout."""
    try:
        with timeout(timeout_seconds):
            # Execute query
            result = execute_query(query)
            return result
    except TimeoutError:
        logger.error(f"Query timed out after {timeout_seconds}s")
        # Return partial results or raise
        raise


def execute_query(query: str):
    """Mock query execution."""
    time.sleep(2)  # Simulate query execution
    return {"status": "success", "rows": 100}


# ============================================================================
# COMPREHENSIVE FAULT TOLERANCE EXAMPLE
# ============================================================================

class FaultTolerantDataPipeline:
    """
    Production-ready data pipeline with multiple fault tolerance mechanisms.
    """
    
    def __init__(self):
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        self.dlq = DeadLetterQueue(storage_path="s3://dlq/pipeline/")
        self.executor = BulkheadExecutor()
    
    @retry_with_exponential_backoff(max_retries=3, base_delay=2)
    def fetch_source_data(self, source_id: str):
        """Fetch data with retry logic."""
        logger.info(f"Fetching data from source: {source_id}")
        
        # Use circuit breaker for external calls
        return self.circuit_breaker.call(
            self._call_external_api,
            source_id
        )
    
    def _call_external_api(self, source_id: str):
        """Call external API (may fail)."""
        # Simulate API call
        if random.random() < 0.1:  # 10% failure rate
            raise ConnectionError("API unavailable")
        return {"data": f"data_from_{source_id}"}
    
    def process_record(self, record: dict):
        """Process individual record with fault tolerance."""
        try:
            # Validate
            self._validate_record(record)
            
            # Transform
            transformed = self._transform_record(record)
            
            # Load
            self._load_record(transformed)
            
            return {"status": "success", "record_id": record.get('id')}
            
        except ValueError as e:
            # Validation errors go to DLQ
            self.dlq.send_to_dlq(
                record, e,
                category='validation_errors',
                metadata={'pipeline': 'main'}
            )
            return {"status": "failed", "reason": "validation"}
            
        except Exception as e:
            # Other errors go to DLQ
            self.dlq.send_to_dlq(
                record, e,
                category='processing_errors',
                metadata={'pipeline': 'main', 'retryable': True}
            )
            return {"status": "failed", "reason": "processing"}
    
    def _validate_record(self, record: dict):
        """Validate record."""
        required_fields = ['id', 'timestamp', 'data']
        for field in required_fields:
            if field not in record:
                raise ValueError(f"Missing required field: {field}")
    
    def _transform_record(self, record: dict):
        """Transform record."""
        return {
            **record,
            'processed_at': datetime.now().isoformat(),
            'pipeline_version': '1.0'
        }
    
    def _load_record(self, record: dict):
        """Load record to destination."""
        # Simulate write operation
        logger.info(f"Loading record: {record['id']}")
    
    def run_pipeline(self, source_ids: list):
        """Run complete pipeline with fault tolerance."""
        results = {
            'success': 0,
            'failed': 0,
            'errors': []
        }
        
        for source_id in source_ids:
            try:
                # Fetch data
                data = self.fetch_source_data(source_id)
                
                # Process records
                for record in data.get('records', []):
                    result = self.process_record(record)
                    
                    if result['status'] == 'success':
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                        
            except Exception as e:
                logger.error(f"Pipeline error for source {source_id}: {e}")
                results['errors'].append({
                    'source_id': source_id,
                    'error': str(e)
                })
        
        logger.info(f"Pipeline completed. Success: {results['success']}, Failed: {results['failed']}")
        return results


# ============================================================================
# FAULT TOLERANCE CHECKLIST
# ============================================================================

"""
FAULT TOLERANCE IMPLEMENTATION CHECKLIST:

✅ Retry Logic
   - Exponential backoff
   - Maximum retry limit
   - Jitter to prevent thundering herd

✅ Circuit Breaker
   - Failure threshold
   - Recovery timeout
   - State management (CLOSED/OPEN/HALF_OPEN)

✅ Dead Letter Queue
   - Categorize failures
   - Persist failed messages
   - Reprocessing mechanism

✅ Checkpointing
   - Regular checkpoint saves
   - Resume from last checkpoint
   - Checkpoint cleanup

✅ Timeouts
   - Set reasonable timeouts
   - Handle timeout exceptions
   - Partial result handling

✅ Graceful Degradation
   - Multiple fallback levels
   - Quality vs availability trade-off
   - Monitor degradation

✅ Bulkhead Pattern
   - Resource isolation
   - Priority-based pools
   - Prevent cascade failures

✅ Monitoring & Alerting
   - Track failure rates
   - Alert on circuit breaker opens
   - DLQ size monitoring
"""

if __name__ == "__main__":
    print("Fault Tolerance Patterns Examples")
    print("Run individual functions to see implementations")
    
    # Example: Run fault-tolerant pipeline
    # pipeline = FaultTolerantDataPipeline()
    # results = pipeline.run_pipeline(['source1', 'source2'])
