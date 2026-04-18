"""
Batch vs Streaming Systems - Practical Examples
================================================
This file demonstrates the differences between batch and streaming processing
with real-world implementations.
"""

# ============================================================================
# BATCH PROCESSING EXAMPLES
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import pandas as pd

# ----------------------------------------------------------------------------
# Example 1: Daily Sales Aggregation (Batch)
# ----------------------------------------------------------------------------

def batch_daily_sales_aggregation():
    """
    Batch job that runs daily to aggregate sales data.
    Processes previous day's complete dataset.
    """
    spark = SparkSession.builder \
        .appName("DailySalesAggregation") \
        .getOrCreate()
    
    # Calculate yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"Processing sales data for {yesterday}")
    
    # Read yesterday's sales data (complete dataset available)
    sales_df = spark.read.parquet(f"s3://raw-data/sales/date={yesterday}")
    
    # Perform complex aggregations
    daily_summary = sales_df.groupBy("store_id", "product_category") \
        .agg(
            sum("amount").alias("total_sales"),
            count("transaction_id").alias("num_transactions"),
            avg("amount").alias("avg_transaction_value"),
            countDistinct("customer_id").alias("unique_customers"),
            max("amount").alias("max_transaction"),
            min("amount").alias("min_transaction")
        )
    
    # Add metadata
    daily_summary = daily_summary.withColumn("processing_date", lit(yesterday)) \
        .withColumn("processed_at", current_timestamp())
    
    # Write to data warehouse (overwrite partition)
    daily_summary.write \
        .mode("overwrite") \
        .partitionBy("processing_date") \
        .parquet("s3://warehouse/daily_sales_summary/")
    
    print(f"Completed processing {daily_summary.count()} aggregated records")
    
    spark.stop()


# ----------------------------------------------------------------------------
# Example 2: Weekly Customer Cohort Analysis (Batch)
# ----------------------------------------------------------------------------

def batch_cohort_analysis():
    """
    Weekly batch job to analyze customer cohorts.
    Requires complete historical data.
    """
    spark = SparkSession.builder \
        .appName("CohortAnalysis") \
        .getOrCreate()
    
    # Read all historical transactions
    transactions = spark.read.parquet("s3://warehouse/transactions/")
    
    # Calculate first purchase date for each customer
    first_purchase = transactions.groupBy("customer_id") \
        .agg(min("transaction_date").alias("cohort_date"))
    
    # Join back to get cohort information
    cohort_data = transactions.join(first_purchase, "customer_id")
    
    # Calculate months since first purchase
    cohort_data = cohort_data.withColumn(
        "months_since_first_purchase",
        months_between(col("transaction_date"), col("cohort_date"))
    )
    
    # Aggregate by cohort and month
    cohort_summary = cohort_data.groupBy("cohort_date", "months_since_first_purchase") \
        .agg(
            countDistinct("customer_id").alias("active_customers"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_revenue_per_customer")
        )
    
    # Calculate retention rates
    cohort_sizes = first_purchase.groupBy("cohort_date") \
        .agg(count("customer_id").alias("cohort_size"))
    
    cohort_retention = cohort_summary.join(cohort_sizes, "cohort_date") \
        .withColumn(
            "retention_rate",
            (col("active_customers") / col("cohort_size")) * 100
        )
    
    # Write results
    cohort_retention.write \
        .mode("overwrite") \
        .parquet("s3://analytics/cohort_analysis/")
    
    spark.stop()


# ----------------------------------------------------------------------------
# Example 3: Machine Learning Model Training (Batch)
# ----------------------------------------------------------------------------

def batch_ml_training():
    """
    Batch job to train ML model on historical data.
    Runs weekly to retrain model with latest data.
    """
    spark = SparkSession.builder \
        .appName("MLModelTraining") \
        .getOrCreate()
    
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import RandomForestRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
    
    # Read training data (last 90 days)
    cutoff_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    
    training_data = spark.read.parquet("s3://features/customer_features/") \
        .filter(col("date") >= cutoff_date)
    
    # Prepare features
    feature_columns = [
        "avg_purchase_value", "purchase_frequency", "days_since_last_purchase",
        "total_lifetime_value", "num_categories_purchased"
    ]
    
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features"
    )
    
    training_data = assembler.transform(training_data)
    
    # Split data
    train, test = training_data.randomSplit([0.8, 0.2], seed=42)
    
    # Train model
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="next_month_revenue",
        numTrees=100
    )
    
    model = rf.fit(train)
    
    # Evaluate
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        labelCol="next_month_revenue",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"Model RMSE: {rmse}")
    
    # Save model
    model.write().overwrite().save("s3://models/customer_ltv_predictor/")
    
    spark.stop()


# ============================================================================
# STREAMING PROCESSING EXAMPLES
# ============================================================================

# ----------------------------------------------------------------------------
# Example 4: Real-Time Fraud Detection (Streaming)
# ----------------------------------------------------------------------------

def streaming_fraud_detection():
    """
    Streaming job to detect fraudulent transactions in real-time.
    Processes events as they arrive with sub-second latency.
    """
    spark = SparkSession.builder \
        .appName("FraudDetection") \
        .getOrCreate()
    
    # Read from Kafka stream
    transactions_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON messages
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("merchant_id", StringType()),
        StructField("location", StringType()),
        StructField("timestamp", TimestampType())
    ])
    
    parsed_stream = transactions_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Add watermark for late data handling
    parsed_stream = parsed_stream.withWatermark("timestamp", "10 minutes")
    
    # Calculate real-time features
    # 1. Transaction velocity (transactions per user in last 5 minutes)
    velocity = parsed_stream.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("user_id")
    ).agg(
        count("transaction_id").alias("txn_count_5min"),
        sum("amount").alias("total_amount_5min")
    )
    
    # 2. Flag suspicious patterns
    suspicious_transactions = parsed_stream.filter(
        (col("amount") > 10000) |  # Large transaction
        (col("location").rlike("high_risk_country"))  # High-risk location
    )
    
    # Join with velocity data
    fraud_alerts = suspicious_transactions.join(
        velocity,
        (suspicious_transactions.user_id == velocity.user_id) &
        (suspicious_transactions.timestamp >= velocity.window.start) &
        (suspicious_transactions.timestamp <= velocity.window.end),
        "left"
    ).select(
        suspicious_transactions["*"],
        coalesce(velocity.txn_count_5min, lit(0)).alias("recent_txn_count")
    ).filter(
        col("recent_txn_count") > 5  # More than 5 transactions in 5 minutes
    )
    
    # Write alerts to Kafka
    query = fraud_alerts.selectExpr(
        "transaction_id as key",
        "to_json(struct(*)) as value"
    ).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "fraud_alerts") \
        .option("checkpointLocation", "s3://checkpoints/fraud_detection/") \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()


# ----------------------------------------------------------------------------
# Example 5: Real-Time Dashboard Metrics (Streaming)
# ----------------------------------------------------------------------------

def streaming_dashboard_metrics():
    """
    Streaming aggregations for real-time dashboard.
    Updates metrics every second.
    """
    spark = SparkSession.builder \
        .appName("RealtimeDashboard") \
        .getOrCreate()
    
    # Read clickstream events
    events_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "clickstream") \
        .load()
    
    # Parse events
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("page_url", StringType()),
        StructField("timestamp", TimestampType())
    ])
    
    parsed_events = events_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Add watermark
    parsed_events = parsed_events.withWatermark("timestamp", "1 minute")
    
    # Calculate real-time metrics
    # 1. Events per minute
    events_per_minute = parsed_events.groupBy(
        window(col("timestamp"), "1 minute", "10 seconds")  # Sliding window
    ).agg(
        count("event_id").alias("event_count"),
        countDistinct("user_id").alias("unique_users")
    )
    
    # 2. Top pages in last 5 minutes
    top_pages = parsed_events.groupBy(
        window(col("timestamp"), "5 minutes"),
        col("page_url")
    ).agg(
        count("event_id").alias("page_views")
    ).orderBy(desc("page_views"))
    
    # Write to Redis for dashboard consumption
    query1 = events_per_minute.writeStream \
        .foreachBatch(lambda df, epoch_id: write_to_redis(df, "metrics:events_per_minute")) \
        .outputMode("update") \
        .option("checkpointLocation", "s3://checkpoints/dashboard_metrics/") \
        .start()
    
    query2 = top_pages.writeStream \
        .foreachBatch(lambda df, epoch_id: write_to_redis(df, "metrics:top_pages")) \
        .outputMode("complete") \
        .option("checkpointLocation", "s3://checkpoints/top_pages/") \
        .start()
    
    query1.awaitTermination()


# ----------------------------------------------------------------------------
# Example 6: Real-Time Recommendation Updates (Streaming)
# ----------------------------------------------------------------------------

def streaming_recommendations():
    """
    Update user recommendations in real-time based on behavior.
    """
    spark = SparkSession.builder \
        .appName("RealtimeRecommendations") \
        .getOrCreate()
    
    # Read user interactions
    interactions = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user_interactions") \
        .load()
    
    # Parse interactions
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
    
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("item_id", StringType()),
        StructField("interaction_type", StringType()),  # view, click, purchase
        StructField("timestamp", TimestampType())
    ])
    
    parsed = interactions.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Calculate user preferences in real-time
    user_preferences = parsed.withWatermark("timestamp", "5 minutes") \
        .groupBy(
            window(col("timestamp"), "30 minutes"),
            col("user_id")
        ).agg(
            collect_list("item_id").alias("recent_items"),
            count("item_id").alias("interaction_count")
        )
    
    # Generate recommendations (simplified)
    recommendations = user_preferences.withColumn(
        "recommended_items",
        udf_get_similar_items(col("recent_items"))
    )
    
    # Write to feature store for online serving
    query = recommendations.writeStream \
        .foreachBatch(update_feature_store) \
        .outputMode("update") \
        .option("checkpointLocation", "s3://checkpoints/recommendations/") \
        .start()
    
    query.awaitTermination()


# ============================================================================
# HYBRID APPROACH - LAMBDA ARCHITECTURE
# ============================================================================

# ----------------------------------------------------------------------------
# Example 7: Lambda Architecture for Analytics
# ----------------------------------------------------------------------------

def lambda_architecture_example():
    """
    Combines batch and streaming for best of both worlds.
    - Batch layer: Accurate historical aggregations
    - Speed layer: Real-time approximate aggregations
    - Serving layer: Merges both views
    """
    
    # BATCH LAYER: Run daily for accurate historical data
    def batch_layer():
        spark = SparkSession.builder \
            .appName("BatchLayer") \
            .getOrCreate()
        
        # Process all historical data
        all_data = spark.read.parquet("s3://data/events/")
        
        # Accurate aggregations
        accurate_metrics = all_data.groupBy("user_id", "date") \
            .agg(
                sum("revenue").alias("total_revenue"),
                count("event_id").alias("event_count")
            )
        
        # Write to batch view
        accurate_metrics.write \
            .mode("overwrite") \
            .partitionBy("date") \
            .parquet("s3://views/batch/user_metrics/")
        
        spark.stop()
    
    # SPEED LAYER: Real-time processing
    def speed_layer():
        spark = SparkSession.builder \
            .appName("SpeedLayer") \
            .getOrCreate()
        
        # Process recent events from stream
        recent_events = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "events") \
            .load()
        
        # Parse and aggregate
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
        
        schema = StructType([
            StructField("user_id", StringType()),
            StructField("event_id", StringType()),
            StructField("revenue", DoubleType()),
            StructField("timestamp", TimestampType())
        ])
        
        parsed = recent_events.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Real-time aggregations (approximate)
        realtime_metrics = parsed.withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "1 day"),
                col("user_id")
            ).agg(
                sum("revenue").alias("total_revenue"),
                count("event_id").alias("event_count")
            )
        
        # Write to speed view
        query = realtime_metrics.writeStream \
            .format("parquet") \
            .option("path", "s3://views/speed/user_metrics/") \
            .option("checkpointLocation", "s3://checkpoints/speed_layer/") \
            .outputMode("update") \
            .start()
        
        query.awaitTermination()
    
    # SERVING LAYER: Merge batch and speed views
    def serve_query(user_id, date):
        """
        Query serving layer that merges batch and speed views.
        """
        spark = SparkSession.builder \
            .appName("ServingLayer") \
            .getOrCreate()
        
        # Read from batch view (accurate historical data)
        batch_data = spark.read.parquet("s3://views/batch/user_metrics/") \
            .filter((col("user_id") == user_id) & (col("date") < date))
        
        # Read from speed view (recent data)
        speed_data = spark.read.parquet("s3://views/speed/user_metrics/") \
            .filter((col("user_id") == user_id) & (col("window.start") >= date))
        
        # Merge results
        total_revenue = batch_data.agg(sum("total_revenue")).collect()[0][0] or 0
        total_revenue += speed_data.agg(sum("total_revenue")).collect()[0][0] or 0
        
        total_events = batch_data.agg(sum("event_count")).collect()[0][0] or 0
        total_events += speed_data.agg(sum("event_count")).collect()[0][0] or 0
        
        spark.stop()
        
        return {
            "user_id": user_id,
            "total_revenue": total_revenue,
            "total_events": total_events
        }
    
    # Run both layers
    import threading
    
    batch_thread = threading.Thread(target=batch_layer)
    speed_thread = threading.Thread(target=speed_layer)
    
    batch_thread.start()
    speed_thread.start()


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def write_to_redis(df, key_prefix):
    """Write streaming DataFrame to Redis."""
    import redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    
    for row in df.collect():
        key = f"{key_prefix}:{row.asDict()}"
        r.set(key, str(row.asDict()))


def update_feature_store(df, epoch_id):
    """Update feature store with latest recommendations."""
    # Write to feature store (e.g., Redis, DynamoDB)
    for row in df.collect():
        user_id = row['user_id']
        recommendations = row['recommended_items']
        # Store in feature store
        print(f"Updated recommendations for user {user_id}: {recommendations}")


def udf_get_similar_items(items):
    """UDF to get similar items based on recent interactions."""
    # Simplified - in production, use collaborative filtering
    from pyspark.sql.functions import udf
    from pyspark.sql.types import ArrayType, StringType
    
    @udf(returnType=ArrayType(StringType()))
    def get_similar(item_list):
        # Mock recommendation logic
        return [f"rec_{item}" for item in item_list[:5]]
    
    return get_similar(items)


# ============================================================================
# COMPARISON SUMMARY
# ============================================================================

"""
BATCH PROCESSING:
- Use Cases: Daily reports, historical analysis, ML training
- Latency: Hours to minutes
- Complexity: Lower
- Cost: Lower
- Examples: Daily sales aggregation, cohort analysis, model training

STREAMING PROCESSING:
- Use Cases: Real-time alerts, live dashboards, fraud detection
- Latency: Seconds to milliseconds
- Complexity: Higher
- Cost: Higher
- Examples: Fraud detection, real-time metrics, recommendations

LAMBDA ARCHITECTURE:
- Use Cases: Need both real-time and accurate historical data
- Combines: Batch accuracy + streaming speed
- Complexity: Highest
- Cost: Highest
- Example: Analytics platform with real-time updates

DECISION CRITERIA:
1. Latency requirements < 1 minute → Streaming
2. Latency requirements > 1 hour → Batch
3. Need both accuracy and speed → Lambda/Kappa
4. Budget constraints → Batch
5. Simple operations → Batch
6. Event-driven architecture → Streaming
"""

if __name__ == "__main__":
    print("Batch vs Streaming Examples")
    print("Run individual functions to see examples")
    
    # Example usage:
    # batch_daily_sales_aggregation()
    # streaming_fraud_detection()
    # lambda_architecture_example()
