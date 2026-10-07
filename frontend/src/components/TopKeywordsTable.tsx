import React from 'react';
import { KeywordItem, TimeWindowLabel, TopNLimit } from '../types/analytics';

interface TopKeywordsTableProps {
  keywords: KeywordItem[];
  topLimit: TopNLimit;
  onTopLimitChange: (limit: TopNLimit) => void;
  activeWindow: TimeWindowLabel;
  onWindowChange: (window: TimeWindowLabel) => void;
  onSelectKeyword?: (keyword: string) => void;
}

const TOP_LIMITS: TopNLimit[] = [5, 10, 20, 50];
const WINDOWS: Array<{ label: string; value: TimeWindowLabel }> = [
  { label: '1m Window', value: '1m' },
  { label: '5m Window', value: '5m' },
  { label: '15m Window', value: '15m' },
  { label: '1h Window', value: '1h' },
];

export const TopKeywordsTable: React.FC<TopKeywordsTableProps> = ({
  keywords,
  topLimit,
  onTopLimitChange,
  activeWindow,
  onWindowChange,
  onSelectKeyword,
}) => {
  const displayed = keywords.slice(0, topLimit);

  return (
    <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg flex flex-col h-full">
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
        <div>
          <h2 className="text-sm font-bold uppercase tracking-wider text-slate-200">
            Top Keywords
          </h2>
          <p className="text-xs text-slate-400">
            Spark Windowed Frequency, Rank &amp; Share
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {/* Time Window Selector */}
          <div className="inline-flex rounded-lg bg-slate-950 p-1 border border-slate-800">
            {WINDOWS.map((w) => (
              <button
                key={w.value}
                type="button"
                onClick={() => onWindowChange(w.value)}
                className={`px-2.5 py-1 rounded-md text-xs font-semibold transition-colors ${
                  activeWindow === w.value
                    ? 'bg-indigo-600 text-white'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                {w.value}
              </button>
            ))}
          </div>

          {/* Top 5 / 10 / 20 / 50 Selector */}
          <select
            aria-label="Select Top Keywords Count"
            value={topLimit}
            onChange={(e) => onTopLimitChange(Number(e.target.value) as TopNLimit)}
            className="bg-slate-950 border border-slate-800 rounded-lg px-2.5 py-1.5 text-xs font-semibold text-slate-200 focus:outline-none focus:border-indigo-500"
          >
            {TOP_LIMITS.map((lim) => (
              <option key={lim} value={lim}>
                Top {lim}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="overflow-y-auto max-h-[340px] border border-slate-800/80 rounded-lg">
        <table className="w-full text-left border-collapse">
          <thead className="bg-slate-950/90 sticky top-0 z-10 text-[11px] font-semibold uppercase tracking-wider text-slate-400 border-b border-slate-800">
            <tr>
              <th className="py-2.5 px-3">Rank</th>
              <th className="py-2.5 px-3">Keyword</th>
              <th className="py-2.5 px-3 text-right">Frequency</th>
              <th className="py-2.5 px-3 text-right">Percentage</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60 text-xs">
            {displayed.length === 0 ? (
              <tr>
                <td colSpan={4} className="py-12 text-center text-slate-500">
                  No keyword statistics yet. Click <span className="text-emerald-400 font-semibold">START</span> to begin streaming.
                </td>
              </tr>
            ) : (
              displayed.map((item) => (
                <tr
                  key={item.keyword}
                  onClick={() => onSelectKeyword?.(item.keyword)}
                  className="hover:bg-slate-800/50 transition-colors cursor-pointer"
                >
                  <td className="py-2.5 px-3 font-bold text-slate-400 tabular-nums">
                    #{item.rank}
                  </td>
                  <td className="py-2.5 px-3 font-semibold text-indigo-300">
                    {item.keyword}
                  </td>
                  <td className="py-2.5 px-3 text-right font-bold text-slate-100 tabular-nums">
                    {item.frequency.toLocaleString()}
                  </td>
                  <td className="py-2.5 px-3 text-right tabular-nums">
                    <div className="inline-flex items-center gap-2">
                      <div className="w-14 bg-slate-800 h-1.5 rounded-full overflow-hidden hidden sm:block">
                        <div
                          className="bg-indigo-500 h-full rounded-full"
                          style={{ width: `${Math.min(100, item.percentage * 3)}%` }}
                        />
                      </div>
                      <span className="text-slate-300 font-medium w-12 text-right">
                        {item.percentage.toFixed(2)}%
                      </span>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};
