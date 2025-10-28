#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution Analysis
1. Count distribution of log levels (INFO, WARN, ERROR, DEBUG)
2. Sample random log entries
3. Generate summary statistics
"""

import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, desc, rand

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,p%(process)s,{%(filename)s:%(lineno)d},%(levelname)s,%(message)s",
)

logger = logging.getLogger(__name__)

# Constants
YOUR_NET_ID = "yl2035"
LOG_ROOT = f"s3a://{YOUR_NET_ID}-assignment-spark-cluster-logs/data"
OUTPUT_DIR = f"s3a://{YOUR_NET_ID}-assignment-spark-cluster-logs/output/problem1"
LOG_LEVEL_PATTERN = r"\b(INFO|WARN|ERROR|DEBUG)\b"

def create_spark_session():
    """Create and configure Spark session for cluster execution."""
    
    logger.info("Creating Spark session for log analysis")
    spark = (SparkSession.builder
        .appName("LogLevelDistribution")
        # Use cluster master
        .master("spark://172.31.88.12:7077")  # MASTER_PRIVATE_IP from cluster-config.txt
        # Cluster-specific configurations
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Memory configurations for cluster
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.executor.cores", "2")
        .getOrCreate()
    )
    
    logger.info("Spark session created successfully")
    return spark

def read_log_files(spark):
    """Read and parse log files from S3."""
    
    logger.info(f"Reading log files from: {LOG_ROOT}")
    try:
        # Read all .log files recursively
        logs_rdd = spark.sparkContext.wholeTextFiles(
            f"{LOG_ROOT}/application_*/container_*.log"
        ).flatMap(lambda file: file[1].splitlines())
        
        # Convert to DataFrame and extract log levels
        logs_df = (logs_rdd.map(lambda line: (line,))
            .toDF(["line"])
            .withColumn("level", regexp_extract(col("line"), LOG_LEVEL_PATTERN, 1))
            .filter(col("level") != "")
        )
        
        total_logs = logs_df.count()
        logger.info(f"Successfully loaded {total_logs:,} log entries")
        return logs_df, total_logs
        
    except Exception as e:
        logger.error(f"Error reading log files: {str(e)}")
        raise

def analyze_log_levels(logs_df):
    """Analyze log level distribution and generate samples."""
    
    logger.info("Starting log level analysis")
    try:
        # Count log levels
        level_counts = (logs_df
            .groupBy("level")
            .agg(count("*").alias("count"))
            .orderBy(desc("count"))
        )
        
        # Sample random logs
        sampled_logs = logs_df.orderBy(rand()).limit(10)
        
        return level_counts, sampled_logs
        
    except Exception as e:
        logger.error(f"Error analyzing log levels: {str(e)}")
        raise

def save_results(spark, level_counts, sampled_logs, total_lines):
    """Save analysis results to S3 and generate summary."""
    
    logger.info(f"Saving results to: {OUTPUT_DIR}")
    try:
        # Save counts
        level_counts.write.csv(
            f"{OUTPUT_DIR}/problem1_counts.csv",
            header=True,
            mode="overwrite"
        )
        
        # Save samples
        sampled_logs.write.csv(
            f"{OUTPUT_DIR}/problem1_sample.csv",
            header=True,
            mode="overwrite"
        )
        
        # Generate summary
        summary = create_summary(level_counts, total_lines)
        
        # Convert summary to DataFrame and save to S3
        summary_path = f"{OUTPUT_DIR}/problem1_summary.txt"
        temp_path = f"{summary_path}_temp"
        
        # Create DataFrame with summary
        summary_rdd = spark.sparkContext.parallelize([summary])
        summary_df = spark.createDataFrame(summary_rdd.map(lambda x: (x,)), ["summary"])
        
        # Write summary using text format
        summary_df.write.mode("overwrite").text(temp_path)
        
        # Rename the part file to summary.txt
        part_file = spark.sparkContext.textFile(f"{temp_path}/part-*").first()
        summary_rdd = spark.sparkContext.parallelize([part_file])
        summary_df = spark.createDataFrame(summary_rdd.map(lambda x: (x,)), ["summary"])
        summary_df.write.mode("overwrite").text(summary_path)
            
        logger.info("Results saved successfully")
        return summary
        
    except Exception as e:
        logger.error(f"Error saving results: {str(e)}")
        raise

def create_summary(level_counts, total_lines):
    """Create a formatted summary of the analysis."""
    
    summary = f"""Log Level Distribution Analysis Summary
===========================================
Total log lines with recognized levels: {total_lines:,}

Log Level Distribution:
"""
    
    for row in level_counts.collect():
        percentage = (row['count'] / total_lines) * 100
        summary += f"{row['level']:<6}: {row['count']:,} ({percentage:.1f}%)\n"
        
    return summary

def main():
    """Main execution function."""
    
    logger.info("Starting Log Level Distribution Analysis")
    print("=" * 70)
    print("PROBLEM 1: Log Level Distribution Analysis")
    print("=" * 70)
    
    start_time = time.time()
    success = False
    
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Read and process log files
        logs_df, total_lines = read_log_files(spark)
        
        # Analyze log levels
        level_counts, sampled_logs = analyze_log_levels(logs_df)
        
        # Save results and create summary
        summary = save_results(spark, level_counts, sampled_logs, total_lines)
        
        # Print results
        print("\nAnalysis Results:")
        print("=" * 70)
        print(summary)
        
        success = True
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        print(f"\n❌ Error: {str(e)}")
    
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")
    
    # Print execution summary
    execution_time = time.time() - start_time
    print("\n" + "=" * 70)
    if success:
        print("✅ ANALYSIS COMPLETED SUCCESSFULLY!")
        print(f"\nTotal execution time: {execution_time:.2f} seconds")
        print("\nFiles created:")
        print(f"  - {OUTPUT_DIR}/problem1_counts.csv")
        print(f"  - {OUTPUT_DIR}/problem1_sample.csv")
        print("  - problem1_summary.txt")
    else:
        print("❌ Analysis failed - check error messages above")
    print("=" * 70)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())