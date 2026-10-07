"""
Member 3 — Spark Text Preprocessing Pipeline.
Implements the 6 mandatory preprocessing stages using native Spark SQL functions:
1. Lowercase conversion
2. Punctuation removal
3. Whitespace normalization
4. Tokenization
5. Stop-word removal
6. Removal of unsuitable short tokens (length < 3 or purely numeric)

Example:
    Input:  "Python is powerful for Big Data Analytics!"
    Output: ["python", "powerful", "big", "data", "analytics"]
"""

import re
from typing import List, Set

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MIN_TOKEN_LENGTH: int = 3

DEFAULT_STOP_WORDS: Set[str] = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can", "cannot", "could", "couldn",
    "did", "didn", "do", "does", "doesn", "doing", "don", "down", "during", "each",
    "few", "for", "from", "further", "had", "hadn", "has", "hasn", "have", "haven",
    "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how",
    "however", "i", "if", "in", "into", "is", "isn", "it", "its", "itself", "just",
    "ll", "m", "ma", "me", "might", "more", "most", "must", "my", "myself", "need",
    "no", "nor", "not", "now", "o", "of", "off", "on", "once", "only", "or", "other",
    "our", "ours", "ourselves", "out", "over", "own", "re", "s", "same", "shan",
    "she", "should", "shouldn", "so", "some", "such", "t", "than", "that", "the",
    "their", "theirs", "them", "themselves", "then", "there", "these", "they",
    "this", "those", "through", "to", "too", "under", "until", "up", "ve", "very",
    "was", "wasn", "we", "were", "weren", "what", "when", "where", "which", "while",
    "who", "whom", "why", "will", "with", "won", "would", "wouldn", "y", "you",
    "your", "yours", "yourself", "yourselves", "also", "using", "used", "via",
    "across", "among", "within", "without", "allow", "allows", "make", "makes",
}


def preprocess_spark_dataframe(
    df: DataFrame,
    text_column: str = "text",
    output_column: str = "keyword",
    min_token_length: int = MIN_TOKEN_LENGTH,
) -> DataFrame:
    """
    Transform a Spark DataFrame containing raw text messages into a DataFrame of
    cleaned, exploded keywords using pure Spark SQL catalyst operations.

    Stages executed in Spark SQL:
    1. Lowercase conversion: `F.lower(F.col(text_column))`
    2. Punctuation removal: `F.regexp_replace(..., r"[^a-z0-9\\s]", " ")`
    3. Whitespace normalization: `F.trim(F.regexp_replace(..., r"\\s+", " "))`
    4. Tokenization: `F.split(..., " ")` followed by `F.explode(...)`
    5. Stop-word removal: `~F.col(output_column).isin(stop_words_list)`
    6. Short/numeric token filtering: `F.length(F.col(output_column)) >= min_token_length`
       and `~F.col(output_column).rlike(r"^\\d+$")`
    """
    stop_words_list = sorted(DEFAULT_STOP_WORDS)

    # 1. Lowercase conversion
    lowercased = F.lower(F.coalesce(F.col(text_column), F.lit("")))

    # 2. Punctuation removal (replace any non-alphanumeric/non-space character with space)
    no_punctuation = F.regexp_replace(lowercased, r"[^a-z0-9\s]", " ")

    # 3. Whitespace normalization (collapse consecutive whitespace and trim)
    normalized_ws = F.trim(F.regexp_replace(no_punctuation, r"\s+", " "))

    # 4. Tokenization & Explode into individual keyword rows
    tokens_df = df.withColumn("_normalized_text", normalized_ws).withColumn(
        "_tokens", F.split(F.col("_normalized_text"), " ")
    )
    exploded_df = tokens_df.withColumn(output_column, F.explode(F.col("_tokens")))

    # 5. Stop-word removal & 6. Removal of unsuitable short or purely numeric tokens
    filtered_df = (
        exploded_df.filter(F.col(output_column).isNotNull())
        .filter(F.length(F.col(output_column)) >= min_token_length)
        .filter(~F.col(output_column).rlike(r"^\d+$"))
        .filter(~F.col(output_column).isin(stop_words_list))
        .drop("_normalized_text", "_tokens")
    )

    return filtered_df


def preprocess_text_string_tokens(
    text: str,
    min_token_length: int = MIN_TOKEN_LENGTH,
) -> List[str]:
    """
    Deterministic reference implementation of the exact 6-step preprocessing specification
    for single-string validation and fast word count telemetry.
    """
    if not text:
        return []
    # 1. Lowercase conversion
    lowered = text.lower()
    # 2. Punctuation removal
    no_punct = re.sub(r"[^a-z0-9\s]", " ", lowered)
    # 3. Whitespace normalization
    normalized = re.sub(r"\s+", " ", no_punct).strip()
    if not normalized:
        return []
    # 4. Tokenization
    raw_tokens = normalized.split(" ")
    # 5. Stop-word removal & 6. Short/numeric token filtering
    cleaned = [
        tok
        for tok in raw_tokens
        if len(tok) >= min_token_length
        and not tok.isdigit()
        and tok not in DEFAULT_STOP_WORDS
    ]
    return cleaned
