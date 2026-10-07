import React, { useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  FrequencyTimelinePoint,
  KeywordItem,
  TrendingKeywordItem,
} from '../types/analytics';

interface FrequencyOverTimeChartProps {
  timeline: FrequencyTimelinePoint[];
  topKeywords: KeywordItem[];
  trending: TrendingKeywordItem[];
}

const LINE_COLORS = ['#6366f1', '#10b981', '#06b6d4', '#f59e0b', '#ec4899'];

export const FrequencyOverTimeChart: React.FC<FrequencyOverTimeChartProps> = ({
  timeline,
  topKeywords,
  trending,
}) => {
  const [viewMode, setViewMode] = useState<'timeline' | 'trending'>('timeline');
  const trackedKeywords = topKeywords.slice(0, 4).map((k) => k.keyword);
  const topTrendingChart = trending.slice(0, 8);

  return (
    <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg flex flex-col h-full">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <div>
          <h2 className="text-sm font-bold uppercase tracking-wider text-slate-200">
            {viewMode === 'timeline'
              ? 'Frequency Over Time'
              : 'Trending Velocity Chart'}
          </h2>
          <p className="text-xs text-slate-400">
            {viewMode === 'timeline'
              ? 'Live sliding window keyword trajectory'
              : 'Top keywords by Change % velocity'}
          </p>
        </div>

        <div className="inline-flex rounded-lg bg-slate-950 p-1 border border-slate-800">
          <button
            type="button"
            onClick={() => setViewMode('timeline')}
            className={`px-2.5 py-1 rounded-md text-xs font-semibold transition-colors ${
              viewMode === 'timeline'
                ? 'bg-indigo-600 text-white'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            Frequency Over Time
          </button>
          <button
            type="button"
            onClick={() => setViewMode('trending')}
            className={`px-2.5 py-1 rounded-md text-xs font-semibold transition-colors ${
              viewMode === 'trending'
                ? 'bg-indigo-600 text-white'
                : 'text-slate-400 hover:text-slate-200'
            }`}
          >
            Trend Change %
          </button>
        </div>
      </div>

      <div className="flex-1 min-h-[290px]">
        {viewMode === 'timeline' ? (
          timeline.length === 0 ? (
            <div className="h-full flex items-center justify-center text-xs text-slate-500 border border-dashed border-slate-800 rounded-lg">
              Waiting for time-series window snapshots...
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={295}>
              <LineChart
                data={timeline}
                margin={{ top: 10, right: 16, left: 0, bottom: 10 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis
                  dataKey="time"
                  stroke="#94a3b8"
                  tick={{ fill: '#94a3b8', fontSize: 11 }}
                />
                <YAxis
                  stroke="#94a3b8"
                  tick={{ fill: '#94a3b8', fontSize: 11 }}
                  allowDecimals={false}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#0f172a',
                    borderColor: '#334155',
                    borderRadius: '0.5rem',
                    fontSize: '12px',
                    color: '#f8fafc',
                  }}
                />
                <Legend wrapperStyle={{ fontSize: '11px' }} />
                <Line
                  type="monotone"
                  dataKey="total_words_1m"
                  name="Total Words (1m)"
                  stroke="#38bdf8"
                  strokeWidth={2}
                  dot={false}
                  isAnimationActive={false}
                />
                {trackedKeywords.map((kw, idx) => (
                  <Line
                    key={kw}
                    type="monotone"
                    dataKey={kw}
                    name={kw}
                    stroke={LINE_COLORS[idx % LINE_COLORS.length]}
                    strokeWidth={2}
                    dot={false}
                    isAnimationActive={false}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          )
        ) : topTrendingChart.length === 0 ? (
          <div className="h-full flex items-center justify-center text-xs text-slate-500 border border-dashed border-slate-800 rounded-lg">
            Waiting for trending keyword comparisons...
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={295}>
            <BarChart
              data={topTrendingChart}
              layout="vertical"
              margin={{ top: 5, right: 20, left: 25, bottom: 5 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis
                type="number"
                stroke="#94a3b8"
                tick={{ fill: '#94a3b8', fontSize: 11 }}
                unit="%"
              />
              <YAxis
                type="category"
                dataKey="keyword"
                stroke="#94a3b8"
                tick={{ fill: '#cbd5e1', fontSize: 11 }}
                width={85}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: '#0f172a',
                  borderColor: '#334155',
                  borderRadius: '0.5rem',
                  fontSize: '12px',
                  color: '#f8fafc',
                }}
                formatter={(val: number) => [`${val.toFixed(1)}%`, 'Change %']}
              />
              <Bar dataKey="change_percentage" radius={[0, 6, 6, 0]} isAnimationActive={false}>
                {topTrendingChart.map((item) => (
                  <Cell
                    key={item.keyword}
                    fill={item.change_percentage >= 0 ? '#10b981' : '#f43f5e'}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
};
