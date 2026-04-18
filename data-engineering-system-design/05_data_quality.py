"""
Data Quality Checks - Production Implementations
================================================
This file demonstrates comprehensive data quality validation patterns.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, mean, stddev, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from typing import Dict, List, Any, Callable
from datetime import datetime, timedelta
from pydantic import BaseModel, validator, EmailStr, conint, confloat
from enum import Enum

# ============================================================================
# PATTERN 1: SCHEMA VALIDATION
# ============================================================================

class UserEventSchema(BaseModel):
    """Pydantic schema for user events with validation rules."""
    
    user_id: str
    email: EmailStr
    age: conint(ge=0, le=150)  # Age between 0 and 150
    signup_date: datetime
    country: str
    revenue: confloat(ge=0)  # Non-negative revenue
    
    @validator('country')
    def country_must_be_valid(cls, v):
        valid_countries = ['US', 'UK', 'CA', 'AU', 'DE', 'FR']
        if v not in valid_countries:
            raise ValueError(f'Country must be one of {valid_countries}')
        return v
    
    @validator('signup_date')
    def signup_date_not_future(cls, v):
        if v > datetime.now():
            raise ValueError('Signup date cannot be in the future')
        return v


def validate_with_pydantic(events: List[Dict]) -> tuple:
    """
    Validate events using Pydantic schema.
    Returns (valid_events, invalid_events_with_errors).
    """
    valid_events = []
    invalid_events = []
    
    for event in events:
        try:
            validated = UserEventSchema(**event)
            valid_events.append(validated.dict())
        except Exception as e:
            invalid_events.append({
                'event': event,
                'error': str(e),
                'error_type': type(e).__name__
            })
    
    print(f"Valid: {len(valid_events)}, Invalid: {len(invalid_events)}")
    return valid_events, invalid_events


def spark_schema_validation():
    """
    Enforce schema in Spark with strict mode.
    """
    spark = SparkSession.builder.appName("SchemaValidation").getOrCreate()
    
    # Define expected schema
    expected_schema = StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("email", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=False),
        StructField("signup_date", TimestampType(), nullable=False),
        StructField("revenue", DoubleType(), nullable=True)
    ])
    
    # Read with schema enforcement
    # PERMISSIVE: Set malformed records to null (default)
    # DROPMALFORMED: Drop malformed records
    # FAILFAST: Fail on first malformed record
    
    df = spark.read \
        .schema(expected_schema) \
        .option("mode", "DROPMALFORMED") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .json("s3://raw/events/")
    
    # Log dropped records
    corrupt_records = df.filter(col("_corrupt_record").isNotNull())
    if corrupt_records.count() > 0:
        corrupt_records.write.mode("append").json("s3://dlq/schema_errors/")
    
    spark.stop()


# ============================================================================
# PATTERN 2: NULL CHECKS
# ============================================================================

class NullChecker:
    """Check for null values in critical columns."""
    
    def __init__(self, critical_columns: List[str], threshold_pct: float = 5.0):
        self.critical_columns = critical_columns
        self.threshold_pct = threshold_pct
    
    def check_nulls(self, df):
        """Check null percentages in critical columns."""
        total_count = df.count()
        
        null_report = []
        
        for col_name in self.critical_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / total_count) * 100
            
            status = "PASS" if null_pct <= self.threshold_pct else "FAIL"
            
            null_report.append({
                'column': col_name,
                'null_count': null_count,
                'null_percentage': round(null_pct, 2),
                'status': status
            })
            
            if status == "FAIL":
                print(f"❌ {col_name}: {null_pct:.2f}% nulls (threshold: {self.threshold_pct}%)")
            else:
                print(f"✅ {col_name}: {null_pct:.2f}% nulls")
        
        return null_report
    
    def remove_nulls(self, df):
        """Remove rows with nulls in critical columns."""
        for col_name in self.critical_columns:
            df = df.filter(col(col_name).isNotNull())
        return df


# ============================================================================
# PATTERN 3: RANGE VALIDATION
# ============================================================================

class RangeValidator:
    """Validate numeric columns are within expected ranges."""
    
    def __init__(self, range_rules: Dict[str, tuple]):
        """
        Args:
            range_rules: Dict mapping column names to (min, max) tuples
        """
        self.range_rules = range_rules
    
    def validate_ranges(self, df):
        """Validate and filter data within ranges."""
        
        for col_name, (min_val, max_val) in self.range_rules.items():
            # Count out-of-range values
            out_of_range = df.filter(
                (col(col_name) < min_val) | (col(col_name) > max_val)
            )
            
            invalid_count = out_of_range.count()
            
            if invalid_count > 0:
                print(f"⚠️  {col_name}: {invalid_count} values out of range [{min_val}, {max_val}]")
                
                # Log invalid records
                out_of_range.write.mode("append") \
                    .parquet(f"s3://dlq/range_errors/{col_name}/")
            
            # Filter to valid range
            df = df.filter(
                (col(col_name) >= min_val) & (col(col_name) <= max_val)
            )
        
        return df


# ============================================================================
# PATTERN 4: UNIQUENESS CHECKS
# ============================================================================

def check_duplicates(df, unique_columns: List[str]):
    """
    Check for duplicate records based on unique columns.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    
    total_count = df.count()
    unique_count = df.select(unique_columns).distinct().count()
    duplicate_count = total_count - unique_count
    
    if duplicate_count > 0:
        print(f"⚠️  Found {duplicate_count} duplicate records")
        
        # Find duplicate groups
        duplicates = df.groupBy(unique_columns) \
            .count() \
            .filter(col("count") > 1) \
            .orderBy(desc("count"))
        
        # Log duplicates
        duplicates.write.mode("overwrite") \
            .parquet("s3://quality/duplicates/")
        
        # Deduplicate - keep first occurrence
        window = Window.partitionBy(unique_columns).orderBy("timestamp")
        df = df.withColumn("row_num", row_number().over(window)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        print(f"✅ Deduplicated: {total_count} -> {df.count()} records")
    
    return df


# ============================================================================
# PATTERN 5: REFERENTIAL INTEGRITY
# ============================================================================

def check_referential_integrity(child_df, parent_df, foreign_key: str, primary_key: str):
    """
    Check if all foreign keys exist in parent table.
    """
    # Find orphaned records
    orphaned = child_df.join(
        parent_df.select(primary_key),
        child_df[foreign_key] == parent_df[primary_key],
        "left_anti"  # Records in child without matching parent
    )
    
    orphaned_count = orphaned.count()
    
    if orphaned_count > 0:
        print(f"❌ Found {orphaned_count} orphaned records")
        
        # Log orphaned records
        orphaned.write.mode("append") \
            .parquet(f"s3://quality/orphaned_{foreign_key}/")
        
        # Option 1: Filter out orphaned records
        child_df = child_df.join(
            parent_df.select(primary_key),
            child_df[foreign_key] == parent_df[primary_key],
            "inner"
        )
        
        print(f"✅ Filtered to {child_df.count()} valid records")
    
    return child_df


# ============================================================================
# PATTERN 6: FRESHNESS CHECKS
# ============================================================================

class FreshnessChecker:
    """Check if data is fresh enough."""
    
    def __init__(self, max_age_hours: int = 24):
        self.max_age_hours = max_age_hours
    
    def check_freshness(self, df, timestamp_column: str):
        """Check data freshness."""
        
        # Get latest timestamp
        latest_ts = df.agg(spark_max(col(timestamp_column))).collect()[0][0]
        
        if latest_ts is None:
            raise ValueError("No data found")
        
        # Calculate age
        age = datetime.now() - latest_ts
        age_hours = age.total_seconds() / 3600
        
        if age_hours > self.max_age_hours:
            print(f"❌ Data is {age_hours:.1f} hours old (threshold: {self.max_age_hours})")
            raise ValueError(f"Data too old: {age_hours:.1f} hours")
        
        print(f"✅ Data freshness OK: {age_hours:.1f} hours old")
        
        # Filter out old data
        cutoff = datetime.now() - timedelta(hours=self.max_age_hours)
        df = df.filter(col(timestamp_column) >= cutoff)
        
        return df


# ============================================================================
# PATTERN 7: STATISTICAL ANOMALY DETECTION
# ============================================================================

class AnomalyDetector:
    """Detect statistical anomalies in numeric columns."""
    
    def __init__(self, std_dev_threshold: float = 3.0):
        self.std_dev_threshold = std_dev_threshold
    
    def detect_anomalies(self, df, column_name: str):
        """Detect outliers using standard deviation method."""
        
        # Calculate statistics
        stats = df.select(
            mean(col(column_name)).alias("mean"),
            stddev(col(column_name)).alias("stddev"),
            spark_min(col(column_name)).alias("min"),
            spark_max(col(column_name)).alias("max"),
            count(col(column_name)).alias("count")
        ).collect()[0]
        
        mean_val = stats['mean']
        std_val = stats['stddev']
        
        if std_val is None or std_val == 0:
            print(f"⚠️  Cannot detect anomalies in {column_name} (no variance)")
            return df
        
        # Define anomaly thresholds
        lower_bound = mean_val - (self.std_dev_threshold * std_val)
        upper_bound = mean_val + (self.std_dev_threshold * std_val)
        
        # Find anomalies
        anomalies = df.filter(
            (col(column_name) < lower_bound) | (col(column_name) > upper_bound)
        )
        
        anomaly_count = anomalies.count()
        
        if anomaly_count > 0:
            print(f"⚠️  Found {anomaly_count} anomalies in {column_name}")
            print(f"   Range: [{lower_bound:.2f}, {upper_bound:.2f}]")
            
            # Log anomalies
            anomalies.write.mode("append") \
                .parquet(f"s3://quality/anomalies/{column_name}/")
        
        return df


# ============================================================================
# PATTERN 8: COMPLETENESS CHECKS
# ============================================================================

def check_completeness(df, date_column: str, expected_count: int = None):
    """
    Check if all expected data is present.
    """
    actual_count = df.count()
    
    # Check record count
    if expected_count and actual_count < expected_count:
        missing_pct = ((expected_count - actual_count) / expected_count) * 100
        print(f"⚠️  Missing {missing_pct:.1f}% of expected records")
        print(f"   Expected: {expected_count}, Actual: {actual_count}")
    
    # Check for date gaps
    date_stats = df.select(
        spark_min(col(date_column)).alias("min_date"),
        spark_max(col(date_column)).alias("max_date")
    ).collect()[0]
    
    min_date = date_stats['min_date']
    max_date = date_stats['max_date']
    
    # Get distinct dates
    actual_dates = set(
        row[date_column].date() 
        for row in df.select(date_column).distinct().collect()
    )
    
    # Generate expected date range
    expected_dates = set()
    current_date = min_date.date()
    while current_date <= max_date.date():
        expected_dates.add(current_date)
        current_date += timedelta(days=1)
    
    # Find missing dates
    missing_dates = expected_dates - actual_dates
    
    if missing_dates:
        print(f"⚠️  Missing data for {len(missing_dates)} dates")
        print(f"   Sample missing dates: {sorted(list(missing_dates))[:5]}")
    else:
        print(f"✅ No date gaps found")
    
    return df


# ============================================================================
# PATTERN 9: COMPREHENSIVE DATA QUALITY FRAMEWORK
# ============================================================================

class DataQualityFramework:
    """
    Comprehensive data quality validation framework.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.quality_metrics = []
    
    def validate_schema(self, df, expected_schema: StructType):
        """Validate DataFrame schema."""
        if df.schema != expected_schema:
            print("❌ Schema mismatch")
            return False
        print("✅ Schema validation passed")
        return True
    
    def validate_nulls(self, df, critical_columns: List[str], threshold: float = 5.0):
        """Validate null percentages."""
        checker = NullChecker(critical_columns, threshold)
        report = checker.check_nulls(df)
        
        failed = [r for r in report if r['status'] == 'FAIL']
        if failed:
            print(f"❌ Null validation failed for {len(failed)} columns")
            return False
        
        print("✅ Null validation passed")
        return True
    
    def validate_ranges(self, df, range_rules: Dict[str, tuple]):
        """Validate numeric ranges."""
        validator = RangeValidator(range_rules)
        df = validator.validate_ranges(df)
        print("✅ Range validation completed")
        return df
    
    def validate_uniqueness(self, df, unique_columns: List[str]):
        """Validate uniqueness."""
        df = check_duplicates(df, unique_columns)
        print("✅ Uniqueness validation completed")
        return df
    
    def validate_freshness(self, df, timestamp_column: str, max_age_hours: int = 24):
        """Validate data freshness."""
        checker = FreshnessChecker(max_age_hours)
        df = checker.check_freshness(df, timestamp_column)
        return df
    
    def detect_anomalies(self, df, numeric_columns: List[str]):
        """Detect statistical anomalies."""
        detector = AnomalyDetector(std_dev_threshold=3.0)
        
        for col_name in numeric_columns:
            df = detector.detect_anomalies(df, col_name)
        
        print("✅ Anomaly detection completed")
        return df
    
    def run_all_checks(self, df, config: Dict[str, Any]):
        """
        Run all quality checks based on configuration.
        """
        print("=" * 60)
        print("STARTING DATA QUALITY VALIDATION")
        print("=" * 60)
        
        # Schema validation
        if 'schema' in config:
            self.validate_schema(df, config['schema'])
        
        # Null checks
        if 'critical_columns' in config:
            self.validate_nulls(df, config['critical_columns'])
        
        # Range validation
        if 'range_rules' in config:
            df = self.validate_ranges(df, config['range_rules'])
        
        # Uniqueness
        if 'unique_columns' in config:
            df = self.validate_uniqueness(df, config['unique_columns'])
        
        # Freshness
        if 'timestamp_column' in config:
            df = self.validate_freshness(
                df,
                config['timestamp_column'],
                config.get('max_age_hours', 24)
            )
        
        # Anomaly detection
        if 'numeric_columns' in config:
            df = self.detect_anomalies(df, config['numeric_columns'])
        
        print("=" * 60)
        print("DATA QUALITY VALIDATION COMPLETED")
        print("=" * 60)
        
        return df


# ============================================================================
# PATTERN 10: GREAT EXPECTATIONS INTEGRATION
# ============================================================================

def great_expectations_validation():
    """
    Use Great Expectations for comprehensive data validation.
    """
    import great_expectations as ge
    import pandas as pd
    
    # Load data
    df = pd.read_csv("data.csv")
    
    # Convert to Great Expectations DataFrame
    df_ge = ge.from_pandas(df)
    
    # Define expectations
    df_ge.expect_column_values_to_not_be_null("user_id")
    df_ge.expect_column_values_to_be_unique("user_id")
    df_ge.expect_column_values_to_be_between("age", min_value=0, max_value=150)
    df_ge.expect_column_values_to_be_in_set("country", ["US", "UK", "CA"])
    df_ge.expect_column_values_to_match_regex(
        "email",
        r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    )
    df_ge.expect_column_mean_to_be_between("revenue", min_value=0, max_value=10000)
    
    # Validate
    results = df_ge.validate()
    
    # Check results
    if results['success']:
        print("✅ All expectations passed")
    else:
        print("❌ Some expectations failed:")
        for result in results['results']:
            if not result['success']:
                print(f"   - {result['expectation_config']['expectation_type']}")
    
    return results


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

def example_usage():
    """Example of using the data quality framework."""
    
    spark = SparkSession.builder.appName("DataQuality").getOrCreate()
    
    # Read data
    df = spark.read.parquet("s3://raw/user_events/")
    
    # Configure quality checks
    quality_config = {
        'critical_columns': ['user_id', 'email', 'timestamp'],
        'range_rules': {
            'age': (0, 150),
            'revenue': (0, 1000000)
        },
        'unique_columns': ['user_id', 'event_id'],
        'timestamp_column': 'timestamp',
        'max_age_hours': 24,
        'numeric_columns': ['revenue', 'age']
    }
    
    # Run quality checks
    framework = DataQualityFramework(spark)
    validated_df = framework.run_all_checks(df, quality_config)
    
    # Write validated data
    validated_df.write.mode("overwrite") \
        .parquet("s3://validated/user_events/")
    
    spark.stop()


# ============================================================================
# DATA QUALITY CHECKLIST
# ============================================================================

"""
DATA QUALITY IMPLEMENTATION CHECKLIST:

✅ Schema Validation
   - Enforce data types
   - Required fields
   - Schema evolution handling

✅ Null Checks
   - Critical columns non-null
   - Null percentage thresholds
   - Log null violations

✅ Range Validation
   - Numeric bounds
   - Date ranges
   - Enum values

✅ Uniqueness
   - Primary key uniqueness
   - Duplicate detection
   - Deduplication strategy

✅ Referential Integrity
   - Foreign key validation
   - Orphaned record handling
   - Cross-table consistency

✅ Freshness
   - Data recency checks
   - SLA monitoring
   - Stale data alerts

✅ Statistical Checks
   - Outlier detection
   - Distribution analysis
   - Trend monitoring

✅ Completeness
   - Expected record counts
   - Date gap detection
   - Missing data alerts

✅ Format Validation
   - Email format
   - Phone numbers
   - URLs, IPs, etc.

✅ Business Rules
   - Domain-specific validations
   - Cross-field validations
   - Conditional rules
"""

if __name__ == "__main__":
    print("Data Quality Patterns Examples")
    print("Run individual functions to see implementations")
