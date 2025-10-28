#!/usr/bin/env python3

import os
import sys
import time
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, input_file_name, to_timestamp, unix_timestamp,
    min as spark_min, max as spark_max, count
)
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Default constants can be overridden via env or CLI
DEFAULT_NET_ID = os.environ.get('YOUR_NET_ID', 'yl2035')
DEFAULT_MASTER_IP = os.environ.get('MASTER_PRIVATE_IP', None)

def create_spark_session(master_url=None):

    if master_url:
        master = master_url
    elif DEFAULT_MASTER_IP:
        # construct spark master url
        master = f"spark://{DEFAULT_MASTER_IP}:7077"
    else:
        logger.warning("No master provided and MASTER_PRIVATE_IP not set; falling back to local[*]")
        master = "local[*]"

    logger.info(f"Creating Spark session with master={master}")
    spark = (SparkSession.builder
             .appName("Problem2_ClusterUsage")
             .master(master)
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.driver.memory", "4g")
             .config("spark.executor.memory", "4g")
             .config("spark.executor.cores", "2")
             .getOrCreate()
             )
    return spark


def read_and_process(spark, log_root_s3):

    logger.info(f"Reading logs from: {log_root_s3}")

    # Read all relevant .log files under application_*/container_*.log
    pattern = f"{log_root_s3}/application_*/container_*.log"
    logs_rdd = spark.sparkContext.wholeTextFiles(pattern).flatMap(lambda file: [(file[0], line) for line in file[1].splitlines()])
    logs_df = logs_rdd.toDF(["file_path", "line"])

    # Extract metadata
    parsed_df = logs_df.select(
        regexp_extract("line", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp_str"),
        input_file_name().alias("file_path"),
        regexp_extract("file_path", r'application_(\d+_\d+)', 1).alias("application_id"),
        regexp_extract("file_path", r'application_(\d+)_\d+', 1).alias("cluster_id"),
        regexp_extract("file_path", r'(container_\d+_\d+_\d+_\d+)', 1).alias("container_id"),
        regexp_extract("line", r'(INFO|WARN|ERROR|DEBUG)', 1).alias("log_level"),
        logs_df["line"].alias("message")
    ).filter("timestamp_str != ''")

    parsed_df = parsed_df.withColumn("timestamp", to_timestamp("timestamp_str", "yy/MM/dd HH:mm:ss"))
    parsed_df.cache()

    # Timeline: app start/end
    timeline_df = parsed_df.groupBy("cluster_id", "application_id").agg(
        spark_min("timestamp").alias("start_time"),
        spark_max("timestamp").alias("end_time")
    )
    timeline_df = timeline_df.withColumn("duration_sec", unix_timestamp("end_time") - unix_timestamp("start_time"))
    timeline_df = timeline_df.withColumn("app_number", regexp_extract("application_id", r'_(\d+)$', 1)).select(
        "cluster_id", "application_id", "app_number", "start_time", "end_time", "duration_sec"
    )

    # Cluster summary
    cluster_summary_df = timeline_df.groupBy("cluster_id").agg(
        count("application_id").alias("num_applications"),
        spark_min("start_time").alias("cluster_first_app"),
        spark_max("end_time").alias("cluster_last_app")
    )

    return timeline_df, cluster_summary_df


def save_outputs(spark, timeline_df, cluster_summary_df, net_id, local_png_dir='.'):

    output_s3 = f"s3a://{net_id}-assignment-spark-cluster-logs/output/problem2"
    logger.info(f"Saving CSV outputs to: {output_s3}")

    timeline_out = f"{output_s3}/problem2_timeline.csv"
    cluster_out = f"{output_s3}/problem2_cluster_summary.csv"

    timeline_df.coalesce(1).write.csv(timeline_out, header=True, mode="overwrite")
    cluster_summary_df.coalesce(1).write.csv(cluster_out, header=True, mode="overwrite")

    # Stats text: create a small summary string and write as text
    timeline_pd = timeline_df.toPandas()
    cluster_pd = cluster_summary_df.toPandas()

    total_clusters = cluster_pd.shape[0]
    total_apps = timeline_pd.shape[0]
    avg_apps = total_apps / total_clusters if total_clusters else 0

    stats = []
    stats.append(f"Total unique clusters: {total_clusters}")
    stats.append(f"Total applications: {total_apps}")
    stats.append(f"Average applications per cluster: {avg_apps:.2f}")
    stats.append("")
    stats.append("Most heavily used clusters:")
    most_used = cluster_pd.sort_values('num_applications', ascending=False)
    for _, r in most_used.iterrows():
        stats.append(f"  Cluster {r['cluster_id']}: {int(r['num_applications'])} applications")

    stats_text = "\n".join(stats)

    # Write stats_text to S3 (as a text file) similar to problem1 pattern
    stats_path = f"{output_s3}/problem2_stats.txt"
    temp_stats = stats_path + "_temp"
    spark.createDataFrame([(stats_text,)], ["summary"]).write.mode('overwrite').text(temp_stats)
    part = spark.sparkContext.textFile(f"{temp_stats}/part-*").first()
    spark.createDataFrame([(part,)], ["summary"]).write.mode('overwrite').text(stats_path)

    logger.info(f"CSV and stats outputs written to S3 at: {output_s3}")

    # Bar chart: applications per cluster
    if not cluster_pd.empty:
        plt.figure(figsize=(8, 5))
        ids = cluster_pd['cluster_id'].astype(str)
        nums = cluster_pd['num_applications']
        bars = plt.bar(ids, nums, color='tab:blue')
        for bar in bars:
            h = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., h, f'{int(h)}', ha='center', va='bottom')
        plt.title('Applications per Cluster')
        plt.xlabel('Cluster ID')
        plt.ylabel('Number of Applications')
        plt.tight_layout()
        bar_png = os.path.join(local_png_dir, 'problem2_bar_chart.png')
        plt.savefig(bar_png)
        plt.close()
        logger.info(f"Saved bar chart to {bar_png}")

    # Histogram of durations for largest cluster
    if not timeline_pd.empty:
        largest_cluster = most_used.iloc[0]['cluster_id']
        largest_pd = timeline_pd[timeline_pd['cluster_id'] == largest_cluster]
        if not largest_pd.empty and largest_pd['duration_sec'].var() > 0:
            plt.figure(figsize=(8,5))
            durations = largest_pd['duration_sec']
            positive = durations[durations > 0]
            if len(positive) > 0:
                bins = np.logspace(np.log10(max(1, positive.min())), np.log10(positive.max()+1), 30)
                plt.hist(positive, bins=bins, color='skyblue', alpha=0.7)
                plt.xscale('log')
                plt.title(f'Job Duration Distribution (Cluster {largest_cluster}, n={len(largest_pd)})')
                plt.xlabel('Duration (seconds)')
                plt.ylabel('Count')
                plt.tight_layout()
                density_png = os.path.join(local_png_dir, 'problem2_density_plot.png')
                plt.savefig(density_png)
                plt.close()
                logger.info(f"Saved density histogram to {density_png}")
            else:
                logger.warning('No positive durations for histogram')
        else:
            logger.warning('No valid durations for density plot (identical or missing)')

    return {
        'timeline_s3': timeline_out,
        'cluster_s3': cluster_out,
        'stats_s3': stats_path
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--master', type=str, default=None, help='Optional full spark master URL (e.g. spark://172.31.88.12:7077)')
    parser.add_argument('--net-id', type=str, default=None, help='Your net id (overrides YOUR_NET_ID env)')
    args = parser.parse_args()

    net_id = args.net_id or DEFAULT_NET_ID

    logger.info('Starting Problem 2 cluster analysis')
    start = time.time()

    spark = None
    try:
        spark = create_spark_session(args.master)

        LOG_ROOT = f"s3a://{net_id}-assignment-spark-cluster-logs/data"
        logger.info(f"Using LOG_ROOT={LOG_ROOT}")

        timeline_df, cluster_summary_df = read_and_process(spark, f"s3a://{net_id}-assignment-spark-cluster-logs/data")

        outputs = save_outputs(spark, timeline_df, cluster_summary_df, net_id, local_png_dir='.')

        logger.info('All outputs created')
        logger.info(outputs)

    except Exception as e:
        logger.exception('Analysis failed')
        print('\n❌ Error: ', str(e))
        return 1

    finally:
        if spark is not None:
            spark.stop()
            logger.info('Spark session stopped')

    elapsed = time.time() - start
    print('\n✅ Completed Problem 2 cluster run')
    print(f'Elapsed time: {elapsed:.1f} seconds')
    print('Outputs (S3):')
    print(f"  - {outputs['timeline_s3']}")
    print(f"  - {outputs['cluster_s3']}")
    print(f"  - {outputs['stats_s3']}")
    print('PNGs saved locally on master: problem2_bar_chart.png, problem2_density_plot.png')

    return 0

if __name__ == '__main__':
    sys.exit(main())
