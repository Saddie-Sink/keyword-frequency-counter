import React from 'react';
import { Activity, Cpu, FileText, Hash, MessageSquare } from 'lucide-react';
import { StreamStatsSummary } from '../types/analytics';

interface SummaryCardsProps {
  stats: StreamStatsSummary;
}

export const SummaryCards: React.FC<SummaryCardsProps> = ({ stats }) => {
  const cards = [
    {
      label: 'Total Messages',
      value: stats.total_messages.toLocaleString(),
      subtext: 'Ingested via Kafka',
      icon: MessageSquare,
      accent: 'text-indigo-400 bg-indigo-500/10 border-indigo-500/20',
    },
    {
      label: 'Total Words',
      value: stats.total_words.toLocaleString(),
      subtext: 'Preprocessed tokens',
      icon: FileText,
      accent: 'text-cyan-400 bg-cyan-500/10 border-cyan-500/20',
    },
    {
      label: 'Unique Keywords',
      value: stats.unique_keywords.toLocaleString(),
      subtext: 'Distinct vocabulary',
      icon: Hash,
      accent: 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20',
    },
    {
      label: 'Messages/sec',
      value: stats.messages_per_sec.toFixed(1),
      subtext: 'Configured target rate',
      icon: Activity,
      accent: 'text-amber-400 bg-amber-500/10 border-amber-500/20',
    },
    {
      label: 'Processing Rate',
      value: `${stats.processing_rate.toFixed(1)} msg/s`,
      subtext: 'Spark streaming throughput',
      icon: Cpu,
      accent: 'text-purple-400 bg-purple-500/10 border-purple-500/20',
    },
  ];

  return (
    <section
      aria-label="Summary Statistics"
      className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4"
    >
      {cards.map((card) => {
        const Icon = card.icon;
        return (
          <div
            key={card.label}
            className="bg-slate-900/90 border border-slate-800 rounded-xl p-4 shadow-lg flex items-center justify-between"
          >
            <div>
              <p className="text-xs font-medium text-slate-400 uppercase tracking-wide">
                {card.label}
              </p>
              <p className="text-2xl font-bold text-slate-100 mt-1 tabular-nums">
                {card.value}
              </p>
              <p className="text-[11px] text-slate-500 mt-1">{card.subtext}</p>
            </div>
            <div className={`p-3 rounded-xl border ${card.accent}`}>
              <Icon className="w-5 h-5" />
            </div>
          </div>
        );
      })}
    </section>
  );
};
