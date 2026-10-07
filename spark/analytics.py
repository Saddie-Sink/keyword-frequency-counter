"""
Member 3 — Spark Structured Streaming Window, Frequency & Trend Analytics.

Implements using native Spark DataFrame and Window operations:
- Keyword Frequency, Percentage, and Rank
- Multi-horizon Time Windows: 1 minute, 5 minutes, 15 minutes, 1 hour
- Watermarking for late-arriving data (with documented rationale)
- Trending Keywords calculation:
    Change % = ((Current - Previous) / Previous) * 100
  with safe zero-division handling when Previous = 0.
"""

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==============================================================================
# WATERMARKING RATIONALE (Requirement 5.E)
# ==============================================================================
# Selected Watermark Threshold: "2 minutes" (`WATERMARK_DURATION = "2 minutes"`)
#
# Justification:
# 1. Late-Data Tolerance: In distributed streaming architectures (Producer -> Kafka -> Spark),
#    network jitter, Kafka partition rebalancing, or micro-batch scheduling delays can cause
#    events to arrive out-of-order after their event-time generation timestamp. A 2-minute
#    watermark allows events arriving up to 120 seconds late to still be accurately aggregated
#    into their respective 1m, 5m, 15m, and 1h event-time windows.
# 2. Bounded Executor Memory: Without watermarking, Spark Structured Streaming must retain
#    intermediate aggregation state for all historical windows indefinitely, leading to
#    OutOfMemoryError (OOM) over long runs. A 2-minute watermark instructs the Spark state store
#    to evict finalized window states older than (max_event_time - 2 minutes).
# ==============================================================================
WATERMARK_DURATION: str = "2 minutes"

# Supported Spark time window specifications (Requirement 5.D)
TIME_WINDOWS: Dict[str, Dict[str, str]] = {
    "1m": {"duration": "1 minute", "slide": "1 minute"},
    "5m": {"duration": "5 minutes", "slide": "1 minute"},
    "15m": {"duration": "15 minutes", "slide": "5 minutes"},
    "1h": {"duration": "1 hour", "slide": "15 minutes"},
}


def apply_watermark(df: DataFrame, timestamp_col: str = "event_time") -> DataFrame:
    """Apply the 2-minute event-time watermark to the streaming DataFrame."""
    return df.withWatermark(timestamp_col, WATERMARK_DURATION)


def compute_keyword_frequency_df(
    tokenized_df: DataFrame,
    keyword_col: str = "keyword",
) -> DataFrame:
    """
    Calculate overall Keyword, Frequency, Percentage, and Rank using pure Spark SQL
    DataFrame and Window operations (Requirement 5.C).

    Columns returned:
    - keyword (string)
    - frequency (long)
    - percentage (double)
    - rank (int)
    """
    # Aggregate frequency per keyword
    freq_df = tokenized_df.groupBy(F.col(keyword_col).alias("keyword")).agg(
        F.count("*").alias("frequency")
    )

    # Global window specifications for total word count and ranking
    total_window = Window.partitionBy()
    rank_window = Window.orderBy(F.desc("frequency"), F.asc("keyword"))

    enriched_df = (
        freq_df.withColumn("_total_words", F.sum("frequency").over(total_window))
        .withColumn(
            "percentage",
            F.when(F.col("_total_words") > 0, F.round((F.col("frequency") / F.col("_total_words")) * 100.0, 2))
            .otherwise(F.lit(0.0)),
        )
        .withColumn("rank", F.row_number().over(rank_window))
        .drop("_total_words")
    )
    return enriched_df


def compute_windowed_keyword_analytics_df(
    tokenized_df: DataFrame,
    window_label: str = "1m",
    timestamp_col: str = "event_time",
    keyword_col: str = "keyword",
) -> DataFrame:
    """
    Compute time-windowed keyword frequency, percentage, and rank for a given
    Spark time window ('1m', '5m', '15m', or '1h') using Spark SQL operations (Requirement 5.D).

    Columns returned:
    - window_label (string)
    - window_start (timestamp)
    - window_end (timestamp)
    - keyword (string)
    - frequency (long)
    - percentage (double)
    - rank (int)
    """
    spec = TIME_WINDOWS.get(window_label, TIME_WINDOWS["1m"])
    window_expr = F.window(F.col(timestamp_col), spec["duration"], spec["slide"])

    grouped_df = (
        tokenized_df.groupBy(window_expr.alias("time_window"), F.col(keyword_col).alias("keyword"))
        .agg(F.count("*").alias("frequency"))
        .select(
            F.lit(window_label).alias("window_label"),
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            F.col("keyword"),
            F.col("frequency"),
        )
    )

    per_window_total = Window.partitionBy("window_label", "window_start", "window_end")
    per_window_rank = Window.partitionBy("window_label", "window_start", "window_end").orderBy(
        F.desc("frequency"), F.asc("keyword")
    )

    result_df = (
        grouped_df.withColumn("_window_total", F.sum("frequency").over(per_window_total))
        .withColumn(
            "percentage",
            F.when(
                F.col("_window_total") > 0,
                F.round((F.col("frequency") / F.col("_window_total")) * 100.0, 2),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("rank", F.row_number().over(per_window_rank))
        .drop("_window_total")
    )

    return result_df


def compute_trending_keywords_df(
    current_window_df: DataFrame,
    previous_window_df: DataFrame,
) -> DataFrame:
    """
    Compute Trending Keywords between the current window and the previous window
    using pure Spark DataFrame operations (Requirement 5.F).

    Formula:
        Change % = ((Current - Previous) / Previous) * 100

    Zero-Division Handling (when Previous == 0):
        If Previous == 0 and Current > 0, Change % is safely set to 100.0 * Current
        (or 100.0 baseline surge indicator) without raising ZeroDivisionError or producing NULL/Infinity.
        Specifically:
        - When previous_frequency == 0 and current_frequency > 0 -> 100.0
        - When previous_frequency == 0 and current_frequency == 0 -> 0.0
        - Otherwise -> round(((current_frequency - previous_frequency) / previous_frequency) * 100.0, 2)

    Columns returned:
    - keyword (string)
    - window_start (timestamp)
    - current_frequency (long)
    - previous_frequency (long)
    - change_percentage (double)
    """
    curr = current_window_df.select(
        F.col("keyword"),
        F.col("window_start"),
        F.col("frequency").alias("current_frequency"),
    )
    prev = previous_window_df.select(
        F.col("keyword"),
        F.col("frequency").alias("previous_frequency"),
    )

    joined = curr.join(prev, on="keyword", how="left").na.fill({"previous_frequency": 0})

    trending_df = joined.withColumn(
        "change_percentage",
        F.when(
            F.col("previous_frequency") == 0,
            F.when(F.col("current_frequency") > 0, F.lit(100.0)).otherwise(F.lit(0.0)),
        ).otherwise(
            F.round(
                ((F.col("current_frequency") - F.col("previous_frequency")) / F.col("previous_frequency"))
                * 100.0,
                2,
            )
        ),
    ).orderBy(F.desc("change_percentage"), F.desc("current_frequency"), F.asc("keyword"))

    return trending_df
