/**
 * Member 5 — TypeScript Interfaces for Real-Time Keyword Frequency & Trend Analytics.
 */

export type StreamLifecycleState = 'STOPPED' | 'RUNNING' | 'PAUSED';

export type TimeWindowLabel = '1m' | '5m' | '15m' | '1h';

export type TopNLimit = 5 | 10 | 20 | 50;

export interface ComponentStatus {
  name: string;
  status: string;
  healthy: boolean;
  details?: Record<string, unknown>;
}

export interface SystemStatus {
  kafka: ComponentStatus;
  spark: ComponentStatus;
  postgresql: ComponentStatus;
  backend: ComponentStatus;
  websocket: ComponentStatus;
  stream_state: StreamLifecycleState;
  timestamp: string;
}

export interface StreamStatus {
  state: StreamLifecycleState;
  rate: number;
  category: string;
  messages_sent_session: number;
  messages_sent_total: number;
  started_at: string | null;
  updated_at: string;
  uptime_seconds: number;
  kafka_connected: boolean;
  kafka_topic: string;
  bootstrap_servers: string;
  last_error: string | null;
}

export interface KeywordItem {
  rank: number;
  keyword: string;
  frequency: number;
  percentage: number;
  window_label: string;
  window_start?: string | null;
  window_end?: string | null;
}

export interface KeywordDetail {
  keyword: string;
  rank: number | null;
  current_frequency: number;
  percentage: number;
  previous_frequency: number;
  change_percentage: number;
  windows: Record<string, number>;
  history: Array<{ time: string; frequency: number }>;
}

export interface TrendingKeywordItem {
  keyword: string;
  current_frequency: number;
  previous_frequency: number;
  change_percentage: number;
  window_start?: string | null;
  direction: 'up' | 'down' | 'stable' | 'new';
}

export interface FrequencyTimelinePoint {
  time: string;
  timestamp: string;
  total_words_1m: number;
  unique_keywords_1m: number;
  [keyword: string]: string | number;
}

export interface StreamStatsSummary {
  total_messages: number;
  total_words: number;
  unique_keywords: number;
  messages_per_sec: number;
  processing_rate: number;
  active_window: string;
  timestamp: string;
  frequency_over_time: FrequencyTimelinePoint[];
}

export interface LiveMessageItem {
  id: string;
  sequence_id: number;
  text: string;
  category: string;
  timestamp: string;
  keywords: string[];
}

export type WebSocketEventType =
  | 'stats_update'
  | 'keyword_update'
  | 'trend_update'
  | 'stream_status'
  | 'system_status'
  | 'new_message'
  | 'error';

export interface WebSocketEnvelope<T = unknown> {
  type: WebSocketEventType;
  data: T;
  timestamp: string;
}
