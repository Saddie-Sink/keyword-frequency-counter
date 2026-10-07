import React, { useCallback, useEffect, useRef, useState } from 'react';
import { AlertCircle, Database, Layers, X } from 'lucide-react';
import { FrequencyOverTimeChart } from './components/FrequencyOverTimeChart';
import { KeywordBarChart } from './components/KeywordBarChart';
import { LiveMessages } from './components/LiveMessages';
import { StreamControls } from './components/StreamControls';
import { SummaryCards } from './components/SummaryCards';
import { SystemStatusBar } from './components/SystemStatusBar';
import { TopKeywordsTable } from './components/TopKeywordsTable';
import { TrendingTable } from './components/TrendingTable';
import { apiClient } from './services/api';
import { AnalyticsWebSocketClient } from './services/websocket';
import {
  KeywordDetail,
  KeywordItem,
  LiveMessageItem,
  StreamLifecycleState,
  StreamStatsSummary,
  StreamStatus,
  SystemStatus,
  TimeWindowLabel,
  TopNLimit,
  TrendingKeywordItem,
  WebSocketEnvelope,
} from './types/analytics';

const INITIAL_STATS: StreamStatsSummary = {
  total_messages: 0,
  total_words: 0,
  unique_keywords: 0,
  messages_per_sec: 0,
  processing_rate: 0,
  active_window: '1m',
  timestamp: new Date().toISOString(),
  frequency_over_time: [],
};

export const App: React.FC = () => {
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null);
  const [wsConnected, setWsConnected] = useState<boolean>(false);
  const [streamState, setStreamState] = useState<StreamLifecycleState>('STOPPED');
  const [streamRate, setStreamRate] = useState<number>(10);
  const [streamCategory, setStreamCategory] = useState<string>('Technology');
  const [stats, setStats] = useState<StreamStatsSummary>(INITIAL_STATS);

  const [keywordsByWindow, setKeywordsByWindow] = useState<
    Record<TimeWindowLabel, KeywordItem[]>
  >({
    '1m': [],
    '5m': [],
    '15m': [],
    '1h': [],
  });
  const [activeWindow, setActiveWindow] = useState<TimeWindowLabel>('1m');
  const [topLimit, setTopLimit] = useState<TopNLimit>(10);
  const [trending, setTrending] = useState<TrendingKeywordItem[]>([]);
  const [liveMessages, setLiveMessages] = useState<LiveMessageItem[]>([]);
  const [selectedKeywordDetail, setSelectedKeywordDetail] =
    useState<KeywordDetail | null>(null);

  const [errorBanner, setErrorBanner] = useState<string | null>(null);
  const [controlLoading, setControlLoading] = useState<boolean>(false);

  const wsRef = useRef<AnalyticsWebSocketClient | null>(null);

  const fetchInitialData = useCallback(async () => {
    try {
      const [sys, strStatus, stSummary, kw1m, trList, msgs] = await Promise.all([
        apiClient.getSystemStatus(),
        apiClient.getStreamStatus(),
        apiClient.getStreamStats('1m'),
        apiClient.getKeywords('1m', 50),
        apiClient.getTrending(25),
        apiClient.getRecentMessages(20),
      ]);
      setSystemStatus(sys);
      setStreamState(strStatus.state);
      setStreamRate(strStatus.rate);
      setStreamCategory(strStatus.category);
      setStats(stSummary);
      setKeywordsByWindow((prev) => ({ ...prev, '1m': kw1m }));
      setTrending(trList);
      setLiveMessages(msgs);
    } catch (err) {
      console.error('Initial data load error:', err);
    }
  }, []);

  useEffect(() => {
    fetchInitialData();

    const wsClient = new AnalyticsWebSocketClient({
      onConnect: () => {
        setWsConnected(true);
      },
      onDisconnect: () => {
        setWsConnected(false);
      },
      onMessage: (envelope: WebSocketEnvelope) => {
        switch (envelope.type) {
          case 'system_status': {
            const sys = envelope.data as SystemStatus;
            setSystemStatus(sys);
            if (sys.stream_state) {
              setStreamState(sys.stream_state);
            }
            break;
          }
          case 'stream_status': {
            const st = envelope.data as StreamStatus;
            setStreamState(st.state);
            setStreamRate(st.rate);
            setStreamCategory(st.category);
            break;
          }
          case 'stats_update': {
            setStats(envelope.data as StreamStatsSummary);
            break;
          }
          case 'keyword_update': {
            const payload = envelope.data as Partial<
              Record<TimeWindowLabel, KeywordItem[]>
            >;
            setKeywordsByWindow((prev) => ({
              ...prev,
              ...payload,
            }));
            break;
          }
          case 'trend_update': {
            setTrending(envelope.data as TrendingKeywordItem[]);
            break;
          }
          case 'new_message': {
            const msg = envelope.data as LiveMessageItem;
            setLiveMessages((prev) => [msg, ...prev.slice(0, 19)]);
            break;
          }
          case 'error': {
            const errObj = envelope.data as { message?: string };
            setErrorBanner(errObj.message || 'An unexpected stream error occurred');
            break;
          }
          default:
            break;
        }
      },
    });

    wsClient.connect();
    wsRef.current = wsClient;

    return () => {
      wsClient.close();
    };
  }, [fetchInitialData]);

  const handleStart = async (rate: number, category: string) => {
    setControlLoading(true);
    setErrorBanner(null);
    try {
      const updated = await apiClient.startStream(rate, category);
      setStreamState(updated.state);
      setStreamRate(updated.rate);
      setStreamCategory(updated.category);
    } catch (err) {
      setErrorBanner(err instanceof Error ? err.message : 'Failed to start stream');
    } finally {
      setControlLoading(false);
    }
  };

  const handlePause = async () => {
    setControlLoading(true);
    setErrorBanner(null);
    try {
      const updated = await apiClient.pauseStream();
      setStreamState(updated.state);
    } catch (err) {
      setErrorBanner(err instanceof Error ? err.message : 'Failed to pause stream');
    } finally {
      setControlLoading(false);
    }
  };

  const handleResume = async () => {
    setControlLoading(true);
    setErrorBanner(null);
    try {
      const updated = await apiClient.resumeStream();
      setStreamState(updated.state);
    } catch (err) {
      setErrorBanner(err instanceof Error ? err.message : 'Failed to resume stream');
    } finally {
      setControlLoading(false);
    }
  };

  const handleStop = async () => {
    setControlLoading(true);
    setErrorBanner(null);
    try {
      const updated = await apiClient.stopStream();
      setStreamState(updated.state);
    } catch (err) {
      setErrorBanner(err instanceof Error ? err.message : 'Failed to stop stream');
    } finally {
      setControlLoading(false);
    }
  };

  const handleWindowChange = async (w: TimeWindowLabel) => {
    setActiveWindow(w);
    try {
      const kwList = await apiClient.getKeywords(w, 50);
      setKeywordsByWindow((prev) => ({ ...prev, [w]: kwList }));
    } catch {
      // handled via websocket fallback
    }
  };

  const handleSelectKeyword = async (keyword: string) => {
    try {
      const detail = await apiClient.getKeywordDetail(keyword);
      setSelectedKeywordDetail(detail);
    } catch (err) {
      setErrorBanner(
        err instanceof Error ? err.message : 'Could not load keyword details'
      );
    }
  };

  const activeKeywords = keywordsByWindow[activeWindow] || [];

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      {/* Top Header */}
      <header className="border-b border-slate-800/80 bg-slate-900/60 backdrop-blur sticky top-0 z-30">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-4 flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="p-2.5 rounded-xl bg-indigo-600/20 border border-indigo-500/30 text-indigo-400">
              <Layers className="w-6 h-6" />
            </div>
            <div>
              <h1 className="text-lg sm:text-xl font-extrabold tracking-tight text-white">
                Real-Time Keyword Analytics
              </h1>
              <p className="text-xs text-slate-400">
                Apache Kafka &bull; Spark Structured Streaming &bull; PostgreSQL &bull; FastAPI &bull; React
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3 text-xs text-slate-400">
            <span className="inline-flex items-center gap-1.5 bg-slate-900 border border-slate-800 px-3 py-1.5 rounded-lg">
              <Database className="w-3.5 h-3.5 text-indigo-400" />
              Watermark: <strong className="text-slate-200">2 minutes</strong>
            </span>
          </div>
        </div>
      </header>

      {/* Main Dashboard Container matching Section 7 Part A layout */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
        {errorBanner && (
          <div className="bg-rose-950/80 border border-rose-500/40 text-rose-200 px-4 py-3 rounded-xl flex items-center justify-between text-xs">
            <div className="flex items-center gap-2">
              <AlertCircle className="w-4 h-4 text-rose-400 shrink-0" />
              <span>{errorBanner}</span>
            </div>
            <button
              type="button"
              onClick={() => setErrorBanner(null)}
              className="text-rose-300 hover:text-white"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
        )}

        {/* Row 1: System Status */}
        <SystemStatusBar systemStatus={systemStatus} wsConnected={wsConnected} />

        {/* Row 2: Summary Cards (Messages | Words | Keywords | Rate | Processing Rate) */}
        <SummaryCards stats={stats} />

        {/* Row 3: Stream Controls */}
        <StreamControls
          streamState={streamState}
          currentRate={streamRate}
          currentCategory={streamCategory}
          onStart={handleStart}
          onPause={handlePause}
          onResume={handleResume}
          onStop={handleStop}
          isLoading={controlLoading}
        />

        {/* Keyword Drilldown Banner (when a keyword row is clicked) */}
        {selectedKeywordDetail && (
          <div className="bg-indigo-950/40 border border-indigo-500/30 rounded-xl p-4 flex flex-wrap items-center justify-between gap-4">
            <div>
              <div className="flex items-center gap-2">
                <span className="text-xs uppercase tracking-wider font-semibold text-indigo-400">
                  Keyword Inspector (`GET /api/keywords/{selectedKeywordDetail.keyword}`)
                </span>
                {selectedKeywordDetail.rank && (
                  <span className="text-xs font-bold bg-indigo-500/20 text-indigo-300 px-2 py-0.5 rounded">
                    Rank #{selectedKeywordDetail.rank}
                  </span>
                )}
              </div>
              <p className="text-base font-bold text-white mt-1">
                &ldquo;{selectedKeywordDetail.keyword}&rdquo; &mdash;{' '}
                <span className="text-indigo-300">
                  {selectedKeywordDetail.current_frequency} occurrences (
                  {selectedKeywordDetail.percentage.toFixed(2)}%)
                </span>
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              {Object.entries(selectedKeywordDetail.windows).map(([w, count]) => (
                <div
                  key={w}
                  className="bg-slate-900/90 border border-slate-800 px-3 py-1.5 rounded-lg text-xs"
                >
                  <span className="text-slate-400 mr-1.5">{w}:</span>
                  <strong className="text-white tabular-nums">{count}</strong>
                </div>
              ))}
              <button
                type="button"
                onClick={() => setSelectedKeywordDetail(null)}
                className="p-1.5 rounded-lg bg-slate-900 hover:bg-slate-800 text-slate-400 hover:text-white"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>
        )}

        {/* Row 4: Top Keywords | Keyword Chart */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <TopKeywordsTable
            keywords={activeKeywords}
            topLimit={topLimit}
            onTopLimitChange={setTopLimit}
            activeWindow={activeWindow}
            onWindowChange={handleWindowChange}
            onSelectKeyword={handleSelectKeyword}
          />
          <KeywordBarChart
            keywords={activeKeywords}
            activeWindow={activeWindow}
          />
        </div>

        {/* Row 5: Trending Keywords | Frequency Over Time */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <TrendingTable
            trending={trending}
            onSelectKeyword={handleSelectKeyword}
          />
          <FrequencyOverTimeChart
            timeline={stats.frequency_over_time}
            topKeywords={activeKeywords}
            trending={trending}
          />
        </div>

        {/* Row 6: Live Messages */}
        <LiveMessages messages={liveMessages} />
      </main>
    </div>
  );
};

export default App;
