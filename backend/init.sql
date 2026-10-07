-- Member 4 — PostgreSQL Schema Initialization (`init.sql`)
-- Creates aggregated analytics tables as specified in Section 6 Part A.
-- Does NOT store unlimited raw messages; stores time-windowed and aggregated statistics.

CREATE TABLE IF NOT EXISTS keyword_statistics (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(128) NOT NULL,
    window_label VARCHAR(16) NOT NULL DEFAULT '1m',
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    frequency INTEGER NOT NULL CHECK (frequency >= 0),
    percentage DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_keyword_stats_keyword ON keyword_statistics(keyword);
CREATE INDEX IF NOT EXISTS idx_keyword_stats_window ON keyword_statistics(window_label, created_at DESC);

CREATE TABLE IF NOT EXISTS trend_statistics (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(128) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    current_frequency INTEGER NOT NULL CHECK (current_frequency >= 0),
    previous_frequency INTEGER NOT NULL CHECK (previous_frequency >= 0),
    change_percentage DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trend_stats_created ON trend_statistics(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trend_stats_keyword ON trend_statistics(keyword);

CREATE TABLE IF NOT EXISTS stream_statistics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    message_count INTEGER NOT NULL DEFAULT 0,
    word_count INTEGER NOT NULL DEFAULT 0,
    unique_keyword_count INTEGER NOT NULL DEFAULT 0,
    processing_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_stream_stats_timestamp ON stream_statistics(timestamp DESC);
