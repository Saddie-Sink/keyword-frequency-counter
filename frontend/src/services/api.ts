/**
 * Member 5 — REST API Client (`services/api.ts`).
 * Communicates with Member 4's FastAPI backend endpoints.
 */

import {
  KeywordDetail,
  KeywordItem,
  LiveMessageItem,
  StreamStatsSummary,
  StreamStatus,
  SystemStatus,
  TimeWindowLabel,
  TrendingKeywordItem,
} from '../types/analytics';

const API_BASE = import.meta.env.VITE_API_BASE_URL || '';

async function requestJson<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${url}`, {
    headers: {
      'Content-Type': 'application/json',
    },
    ...options,
  });
  if (!response.ok) {
    let errorDetail = `HTTP ${response.status}`;
    try {
      const errBody = await response.json();
      if (errBody.detail) {
        errorDetail = errBody.detail;
      }
    } catch {
      // ignore json parse error
    }
    throw new Error(errorDetail);
  }
  return response.json() as Promise<T>;
}

export const apiClient = {
  getSystemStatus(): Promise<SystemStatus> {
    return requestJson<SystemStatus>('/api/status');
  },

  getStreamStats(window: TimeWindowLabel = '1m'): Promise<StreamStatsSummary> {
    return requestJson<StreamStatsSummary>(`/api/stats?window=${encodeURIComponent(window)}`);
  },

  getKeywords(window: TimeWindowLabel = '1m', limit: number = 20): Promise<KeywordItem[]> {
    return requestJson<KeywordItem[]>(
      `/api/keywords?window=${encodeURIComponent(window)}&limit=${limit}`
    );
  },

  getKeywordDetail(keyword: string): Promise<KeywordDetail> {
    return requestJson<KeywordDetail>(`/api/keywords/${encodeURIComponent(keyword)}`);
  },

  getTrending(limit: number = 15): Promise<TrendingKeywordItem[]> {
    return requestJson<TrendingKeywordItem[]>(`/api/trending?limit=${limit}`);
  },

  getRecentMessages(limit: number = 20): Promise<LiveMessageItem[]> {
    return requestJson<LiveMessageItem[]>(`/api/messages?limit=${limit}`);
  },

  getStreamStatus(): Promise<StreamStatus> {
    return requestJson<StreamStatus>('/api/stream/status');
  },

  startStream(rate: number, category: string): Promise<StreamStatus> {
    return requestJson<StreamStatus>('/api/stream/start', {
      method: 'POST',
      body: JSON.stringify({ rate, category }),
    });
  },

  pauseStream(): Promise<StreamStatus> {
    return requestJson<StreamStatus>('/api/stream/pause', {
      method: 'POST',
    });
  },

  resumeStream(): Promise<StreamStatus> {
    return requestJson<StreamStatus>('/api/stream/resume', {
      method: 'POST',
    });
  },

  stopStream(): Promise<StreamStatus> {
    return requestJson<StreamStatus>('/api/stream/stop', {
      method: 'POST',
    });
  },
};
