import React from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { KeywordItem, TimeWindowLabel } from '../types/analytics';

interface KeywordBarChartProps {
  keywords: KeywordItem[];
  activeWindow: TimeWindowLabel;
}

const BAR_COLORS = [
  '#6366f1',
  '#8b5cf6',
  '#06b6d4',
  '#10b981',
  '#3b82f6',
  '#a855f7',
  '#14b8a6',
  '#f59e0b',
  '#ec4899',
  '#64748b',
];

export const KeywordBarChart: React.FC<KeywordBarChartProps> = ({
  keywords,
  activeWindow,
}) => {
  const chartData = keywords.slice(0, 10);

  return (
    <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg flex flex-col h-full">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-sm font-bold uppercase tracking-wider text-slate-200">
            Keyword Chart ({activeWindow} Window)
          </h2>
          <p className="text-xs text-slate-400">
            Real-time top 10 keyword frequency distribution
          </p>
        </div>
      </div>

      <div className="flex-1 min-h-[300px]">
        {chartData.length === 0 ? (
          <div className="h-full flex items-center justify-center text-xs text-slate-500 border border-dashed border-slate-800 rounded-lg">
            Waiting for Spark Structured Streaming keyword aggregations...
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={310}>
            <BarChart
              data={chartData}
              margin={{ top: 10, right: 16, left: 0, bottom: 28 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis
                dataKey="keyword"
                stroke="#94a3b8"
                tick={{ fill: '#cbd5e1', fontSize: 11 }}
                angle={-25}
                textAnchor="end"
                interval={0}
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
                formatter={(value: number, _name: string, props: { payload?: KeywordItem }) => [
                  `${value} (${props.payload?.percentage?.toFixed(2) ?? 0}%)`,
                  'Frequency',
                ]}
              />
              <Bar dataKey="frequency" radius={[6, 6, 0, 0]} isAnimationActive={false}>
                {chartData.map((entry, index) => (
                  <Cell
                    key={entry.keyword}
                    fill={BAR_COLORS[index % BAR_COLORS.length]}
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
