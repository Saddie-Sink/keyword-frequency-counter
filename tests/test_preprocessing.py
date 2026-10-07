"""
Member 3 & Member 6 — Text Preprocessing Pipeline Tests.
Verifies:
1. Lowercase conversion
2. Punctuation removal
3. Whitespace normalization
4. Tokenization
5. Stop-word removal
6. Removal of unsuitable short tokens (< 3 chars) and purely numeric tokens
"""

from spark.preprocessing import preprocess_text_string_tokens


def test_specification_example_preprocessing():
    """
    Verify exact example from Section 5 Part B:
    Input:  "Python is powerful for Big Data Analytics!"
    Output: ["python", "powerful", "big", "data", "analytics"]
    """
    raw_input = "Python is powerful for Big Data Analytics!"
    tokens = preprocess_text_string_tokens(raw_input)
    assert tokens == ["python", "powerful", "big", "data", "analytics"]


def test_lowercase_and_punctuation_removal():
    raw_input = "KAFKA, Spark-Streaming... & PostgreSQL!! @2026 #Realtime"
    tokens = preprocess_text_string_tokens(raw_input)
    assert "kafka" in tokens
    assert "spark" in tokens
    assert "streaming" in tokens
    assert "postgresql" in tokens
    assert "realtime" in tokens
    # Purely numeric token "2026" is removed
    assert "2026" not in tokens


def test_whitespace_normalization_and_short_token_removal():
    raw_input = "   AI   is   an   ml  and   dl   system   with   gpu   clusters   "
    tokens = preprocess_text_string_tokens(raw_input)
    # 'ai', 'is', 'an', 'ml', 'dl' are < 3 chars or stop words; 'system', 'gpu', 'clusters' remain
    assert tokens == ["system", "gpu", "clusters"]


def test_stopword_removal_and_empty_inputs():
    assert preprocess_text_string_tokens("") == []
    assert preprocess_text_string_tokens("   !@#$%^&*()   ") == []
    assert preprocess_text_string_tokens("the and or if but in on at to for with") == []
