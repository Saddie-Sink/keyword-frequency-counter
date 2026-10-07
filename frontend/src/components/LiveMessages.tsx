import React from 'react';
import { Radio } from 'lucide-react';
import { LiveMessageItem } from '../types/analytics';

interface LiveMessagesProps {
  messages: LiveMessageItem[];
}

const CATEGORY_COLORS: Record<string, string> = {
  Technology: 'bg-indigo-500/15 text-indigo-300 border-indigo-500/30',
  'Big Data': 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30',
  'Artificial Intelligence': 'bg-purple-500/15 text-purple-300 border-purple-500/30',
  'Data Science': 'bg-cyan-500/15 text-cyan-300 border-cyan-500/30',
  News: 'bg-amber-500/15 text-amber-300 border-amber-500/30',
  'Product Reviews': 'bg-pink-500/15 text-pink-300 border-pink-500/30',
};

export const LiveMessages: React.FC<LiveMessagesProps> = ({ messages }) => {
  // Requirement 7 Part H: Display a limited number of recent messages (do not load entire history)
  const boundedMessages = messages.slice(0, 20);

  return (
    <section
      aria-label="Live Messages"
      className="cv-auto-section bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg"
    >
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2.5">
          <Radio className="w-4 h-4 text-indigo-400" />
          <div>
            <h2 className="text-sm font-bold uppercase tracking-wider text-slate-200">
              Live Messages (Kafka Topic: <code className="text-indigo-400">text_stream</code>)
            </h2>
            <p className="text-xs text-slate-400">
              Bounded recent stream buffer (latest {boundedMessages.length} of max 20 displayed)
            </p>
          </div>
        </div>
      </div>

      <div className="space-y-2.5 max-h-[320px] overflow-y-auto pr-1">
        {boundedMessages.length === 0 ? (
          <div className="py-10 text-center text-xs text-slate-500 border border-dashed border-slate-800 rounded-lg">
            No live messages received yet. Configure rate &amp; category above and click{' '}
            <span className="text-emerald-400 font-semibold">START</span>.
          </div>
        ) : (
          boundedMessages.map((msg) => {
            const badgeStyle =
              CATEGORY_COLORS[msg.category] ||
              'bg-slate-800 text-slate-300 border-slate-700';
            const timeFormatted = new Date(msg.timestamp).toLocaleTimeString();

            return (
              <div
                key={msg.id}
                className="bg-slate-950/80 border border-slate-800/80 rounded-lg p-3 flex flex-col sm:flex-row sm:items-center justify-between gap-2"
              >
                <div className="space-y-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-[11px] font-mono text-slate-500">
                      #{msg.sequence_id}
                    </span>
                    <span
                      className={`text-[11px] font-semibold px-2 py-0.5 rounded border ${badgeStyle}`}
                    >
                      {msg.category}
                    </span>
                    <span className="text-[11px] text-slate-500 font-mono">
                      {timeFormatted}
                    </span>
                  </div>
                  <p className="text-xs text-slate-200 leading-relaxed">{msg.text}</p>
                </div>

                {msg.keywords && msg.keywords.length > 0 && (
                  <div className="flex flex-wrap gap-1 sm:justify-end max-w-xs">
                    {msg.keywords.slice(0, 6).map((kw, i) => (
                      <span
                        key={`${msg.id}-${kw}-${i}`}
                        className="text-[10px] font-mono bg-slate-900 border border-slate-800 text-slate-400 px-1.5 py-0.5 rounded"
                      >
                        {kw}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>
    </section>
  );
};
