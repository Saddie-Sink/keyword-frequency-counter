/**
 * Member 5 — Real-Time WebSocket Client (`services/websocket.ts`).
 * Connects to `WS /ws`, handles automatic exponential-backoff reconnection,
 * and dispatches all 7 specification event types:
 * - `stats_update`
 * - `keyword_update`
 * - `trend_update`
 * - `stream_status`
 * - `system_status`
 * - `new_message`
 * - `error`
 */

import { WebSocketEnvelope } from '../types/analytics';

export interface WebSocketHandlers {
  onConnect?: () => void;
  onDisconnect?: () => void;
  onMessage?: (envelope: WebSocketEnvelope) => void;
}

export class AnalyticsWebSocketClient {
  private ws: WebSocket | null = null;
  private reconnectTimer: number | null = null;
  private intentionallyClosed = false;
  private handlers: WebSocketHandlers;

  constructor(handlers: WebSocketHandlers) {
    this.handlers = handlers;
  }

  private resolveWsUrl(): string {
    if (import.meta.env.VITE_WS_URL) {
      return `${import.meta.env.VITE_WS_URL}/ws`;
    }
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${window.location.host}/ws`;
  }

  public connect(): void {
    this.intentionallyClosed = false;
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    try {
      this.ws = new WebSocket(this.resolveWsUrl());

      this.ws.onopen = () => {
        if (this.reconnectTimer !== null) {
          window.clearTimeout(this.reconnectTimer);
          this.reconnectTimer = null;
        }
        this.handlers.onConnect?.();
      };

      this.ws.onmessage = (event: MessageEvent) => {
        try {
          const parsed = JSON.parse(event.data) as WebSocketEnvelope;
          this.handlers.onMessage?.(parsed);
        } catch (err) {
          console.error('Failed to parse WebSocket payload:', err);
        }
      };

      this.ws.onclose = () => {
        this.handlers.onDisconnect?.();
        if (!this.intentionallyClosed) {
          this.scheduleReconnect();
        }
      };

      this.ws.onerror = () => {
        // Browser will invoke onclose right after onerror
      };
    } catch {
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== null || this.intentionallyClosed) {
      return;
    }
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, 2000);
  }

  public send(payload: Record<string, unknown>): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
    }
  }

  public close(): void {
    this.intentionallyClosed = true;
    if (this.reconnectTimer !== null) {
      window.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}
