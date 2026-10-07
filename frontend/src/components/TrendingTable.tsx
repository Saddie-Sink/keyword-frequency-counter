import React from 'react';
import { ArrowDownRight, ArrowUpRight, Minus, Sparkles } from 'lucide-react';
import { TrendingKeywordItem } from '../types/analytics';

interface TrendingTableProps {
  trending: TrendingKeywordItem[];
  onSelectKeyword?: (keyword: string) => void;
}

export const TrendingTable: React.FC<TrendingTableProps> = ({
  trending,
  onSelectKeyword,
}) => {
  return (
    <div className="bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg flex flex-col h-full">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-sm font-bold uppercase tracking-wider text-slate-200">
            Trending Keywords
          </h2>
          <p className="text-xs text-slate-400">
            Change % = ((Current - Previous) / Previous) &times; 100
          </p>
        </div>
      </div>

      <div className="overflow-y-auto max-h-[320px] border border-slate-800/80 rounded-lg">
        <table className="w-full text-left border-collapse">
          <thead className="bg-slate-950/90 sticky top-0 z-10 text-[11px] font-semibold uppercase tracking-wider text-slate-400 border-b border-slate-800">
            <tr>
              <th className="py-2.5 px-3">Keyword</th>
              <th className="py-2.5 px-3 text-right">Current Frequency</th>
              <th className="py-2.5 px-3 text-right">Previous Frequency</th>
              <th className="py-2.5 px-3 text-right">Change %</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60 text-xs">
            {trending.length === 0 ? (
              <tr>
                <td colSpan={4} className="py-12 text-center text-slate-500">
                  No trend data computed yet. Start the stream to track keyword velocity.
                </td>
              </tr>
            ) : (
              trending.map((item) => {
                const isPositive = item.change_percentage > 0;
                const isNegative = item.change_percentage < 0;
                const isNew = item.previous_frequency === 0 && item.current_frequency > 0;

                return (
                  <tr
                    key={item.keyword}
                    onClick={() => onSelectKeyword?.(item.keyword)}
                    className="hover:bg-slate-800/50 transition-colors cursor-pointer"
                  >
                    <td className="py-2.5 px-3 font-semibold text-slate-200">
                      {item.keyword}
                    </td>
                    <td className="py-2.5 px-3 text-right font-bold text-slate-100 tabular-nums">
                      {item.current_frequency}
                    </td>
                    <td className="py-2.5 px-3 text-right text-slate-400 tabular-nums">
                      {item.previous_frequency}
                    </td>
                    <td className="py-2.5 px-3 text-right tabular-nums">
                      <span
                        className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-md font-bold ${
                          isNew
                            ? 'bg-indigo-500/15 text-indigo-400 border border-indigo-500/30'
                            : isPositive
                            ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30'
                            : isNegative
                            ? 'bg-rose-500/15 text-rose-400 border border-rose-500/30'
                            : 'bg-slate-800 text-slate-400'
                        }`}
                      >
                        {isNew ? (
                          <Sparkles className="w-3 h-3" />
                        ) : isPositive ? (
                          <ArrowUpRight className="w-3.5 h-3.5" />
                        ) : isNegative ? (
                          <ArrowDownRight className="w-3.5 h-3.5" />
                        ) : (
                          <Minus className="w-3 h-3" />
                        )}
                        {isPositive ? '+' : ''}
                        {item.change_percentage.toFixed(1)}%
                      </span>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};
