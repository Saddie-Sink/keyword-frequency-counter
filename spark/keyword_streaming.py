"""
Member 3 — Main Spark Structured Streaming Job (`keyword_streaming.py`).
Pipeline:
    Apache Kafka (`text_stream`)
        ↓
    Spark Structured Streaming (`readStream`)
        ↓
    JSON Message Extraction & Event-Time Watermarking (`2 minutes`)
        ↓
    Spark SQL Text Preprocessing (lowercase, punctuation, whitespace, tokenize, stopwords, length>=3)
        ↓
    Spark Multi-Window Aggregations (1m, 5m, 15m, 1h) & Trending Calculation
        ↓
    PostgreSQL Persistence (`keyword_statistics`, `trend_statistics`, `stream_statistics`)
    with Fault-Tolerant Spark Checkpointing (`checkpointLocation`)
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark.analytics import (
    TIME_WINDOWS,
    WATERMARK_DURATION,
    compute_trending_keywords_df,
    compute_windowed_keyword_analytics_df,
)
from spark.preprocessing import preprocess_spark_dataframe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("spark.keyword_streaming")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "text_stream")
CHECKPOINT_LOCATION = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark_checkpoints/keyword_analytics")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "keyword_analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

MESSAGE_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("sequence_id", IntegerType(), True),
        StructField("text", StringType(), True),
        StructField("category", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    """Create and configure the SparkSession for Kafka Structured Streaming."""
    packages = os.getenv(
        "SPARK_JARS_PACKAGES",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
    )
    builder = (
        SparkSession.builder.appName("RealTimeKeywordFrequencyAndTrendAnalysis")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "SparkSession initialized (version=%s, watermark=%s, checkpoint=%s)",
        spark.version,
        WATERMARK_DURATION,
        CHECKPOINT_LOCATION,
    )
    return spark


def get_pg_connection():
    """Return a psycopg2 connection to PostgreSQL."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=5,
    )


class StatefulBatchProcessor:
    """
    Handles Spark Structured Streaming `foreachBatch` execution:
    - Executes Spark DataFrame preprocessing, window aggregations (1m, 5m, 15m, 1h),
      and trend calculations against the previous window state in Spark.
    - Persists aggregated metrics to PostgreSQL (`keyword_statistics`, `trend_statistics`,
      `stream_statistics`).
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.previous_window_df: DataFrame = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("keyword", StringType(), True),
                    StructField("frequency", IntegerType(), True),
                ]
            ),
        )
        self.total_messages_processed: int = 0
        self.total_words_processed: int = 0

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        start_time = time.perf_counter()
        row_count = batch_df.count()
        if row_count == 0:
            return

        logger.info("Processing Spark micro-batch #%d with %d raw messages...", batch_id, row_count)

        # Cache batch DataFrame for multi-window analytical passes
        batch_df.persist()
        try:
            # Step B: Text Preprocessing using Spark SQL functions
            tokenized_df = preprocess_spark_dataframe(batch_df, text_column="text", output_column="keyword")
            tokenized_df.persist()

            batch_word_count = tokenized_df.count()
            unique_keywords_in_batch = tokenized_df.select("keyword").distinct().count()

            self.total_messages_processed += row_count
            self.total_words_processed += batch_word_count

            # Step C & D: Multi-horizon Window Analytics (1m, 5m, 15m, 1h)
            keyword_rows_to_insert = []
            now_utc = datetime.now(timezone.utc)

            latest_1m_df = None
            for window_label in TIME_WINDOWS:
                windowed_df = compute_windowed_keyword_analytics_df(
                    tokenized_df,
                    window_label=window_label,
                    timestamp_col="event_time",
                    keyword_col="keyword",
                )
                if window_label == "1m":
                    latest_1m_df = windowed_df

                collected = windowed_df.limit(100).collect()
                for r in collected:
                    keyword_rows_to_insert.append(
                        (
                            r["keyword"],
                            window_label,
                            r["window_start"],
                            r["window_end"],
                            int(r["frequency"]),
                            float(r["percentage"]),
                            now_utc,
                        )
                    )

            # Step F: Trending Keywords calculation using Spark DataFrames
            trend_rows_to_insert = []
            if latest_1m_df is not None:
                current_agg_df = (
                    latest_1m_df.groupBy("keyword")
                    .agg(
                        F.max("window_start").alias("window_start"),
                        F.sum("frequency").alias("frequency"),
                    )
                )
                trending_df = compute_trending_keywords_df(current_agg_df, self.previous_window_df)
                for r in trending_df.limit(50).collect():
                    trend_rows_to_insert.append(
                        (
                            r["keyword"],
                            r["window_start"] or now_utc,
                            int(r["current_frequency"]),
                            int(r["previous_frequency"]),
                            float(r["change_percentage"]),
                            now_utc,
                        )
                    )
                # Update previous window reference for next micro-batch comparison
                self.previous_window_df = current_agg_df.select(
                    F.col("keyword"),
                    F.col("frequency").cast(IntegerType()).alias("frequency"),
                )

            elapsed = max(0.001, time.perf_counter() - start_time)
            processing_rate = round(row_count / elapsed, 2)

            # Persist aggregated results to PostgreSQL
            self._write_to_postgres(
                keyword_rows=keyword_rows_to_insert,
                trend_rows=trend_rows_to_insert,
                message_count=self.total_messages_processed,
                word_count=self.total_words_processed,
                unique_keyword_count=unique_keywords_in_batch,
                processing_rate=processing_rate,
                now_utc=now_utc,
            )

            logger.info(
                "Completed micro-batch #%d in %.2fs (messages=%d, words=%d, rate=%.2f msg/s)",
                batch_id,
                elapsed,
                row_count,
                batch_word_count,
                processing_rate,
            )
            tokenized_df.unpersist()
        except Exception as exc:
            logger.exception("Error processing Spark micro-batch #%d: %s", batch_id, exc)
        finally:
            batch_df.unpersist()

    def _write_to_postgres(
        self,
        keyword_rows,
        trend_rows,
        message_count: int,
        word_count: int,
        unique_keyword_count: int,
        processing_rate: float,
        now_utc: datetime,
    ) -> None:
        try:
            conn = get_pg_connection()
            with conn:
                with conn.cursor() as cur:
                    if keyword_rows:
                        execute_values(
                            cur,
                            """
                            INSERT INTO keyword_statistics
                                (keyword, window_label, window_start, window_end, frequency, percentage, created_at)
                            VALUES %s
                            """,
                            keyword_rows,
                        )
                    if trend_rows:
                        execute_values(
                            cur,
                            """
                            INSERT INTO trend_statistics
                                (keyword, window_start, current_frequency, previous_frequency, change_percentage, created_at)
                            VALUES %s
                            """,
                            trend_rows,
                        )
                    cur.execute(
                        """
                        INSERT INTO stream_statistics
                            (timestamp, message_count, word_count, unique_keyword_count, processing_rate)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (now_utc, message_count, word_count, unique_keyword_count, processing_rate),
                    )
            conn.close()
        except Exception as db_exc:
            logger.error("PostgreSQL write error in Spark batch: %s", db_exc)


def run_streaming_pipeline() -> None:
    """Main entrypoint for the Spark Structured Streaming pipeline."""
    os.makedirs(CHECKPOINT_LOCATION, exist_ok=True)
    spark = create_spark_session()

    logger.info(
        "Connecting Spark Structured Streaming to Kafka bootstrap_servers=%s, topic=%s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
    )

    # Step A: Kafka -> Spark readStream
    raw_kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Extract JSON payload and event timestamp + Step E: Apply Watermarking
    parsed_stream = (
        raw_kafka_stream.selectExpr("CAST(value AS STRING) AS json_payload", "timestamp AS kafka_timestamp")
        .select(
            F.from_json(F.col("json_payload"), MESSAGE_SCHEMA).alias("data"),
            F.col("kafka_timestamp"),
        )
        .select(
            F.col("data.id").alias("id"),
            F.col("data.text").alias("text"),
            F.col("data.category").alias("category"),
            F.coalesce(
                F.to_timestamp(F.col("data.timestamp")),
                F.col("kafka_timestamp"),
                F.current_timestamp(),
            ).alias("event_time"),
        )
        .filter(F.col("text").isNotNull())
        .withWatermark("event_time", WATERMARK_DURATION)
    )

    processor = StatefulBatchProcessor(spark)

    # Step G: Configure checkpointLocation and start Structured Streaming query
    query = (
        parsed_stream.writeStream.foreachBatch(processor.process_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="3 seconds")
        .start()
    )

    logger.info("Spark Structured Streaming query is active and awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming_pipeline()
