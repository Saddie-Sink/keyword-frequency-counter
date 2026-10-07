import React from 'react';
import { SystemStatus } from '../types/analytics';

interface SystemStatusBarProps {
  systemStatus: SystemStatus | null;
  wsConnected: boolean;
}

export const SystemStatusBar: React.FC<SystemStatusBarProps> = ({
  systemStatus,
  wsConnected,
}) => {
  const items = [
    {
      name: 'Kafka',
      status: systemStatus?.kafka.status || 'Connected',
      healthy: systemStatus ? systemStatus.kafka.healthy : true,
    },
    {
      name: 'Spark',
      status: systemStatus?.spark.status || 'Ready',
      healthy: systemStatus ? systemStatus.spark.healthy : true,
    },
    {
      name: 'PostgreSQL',
      status: systemStatus?.postgresql.status || 'Connected',
      healthy: systemStatus ? systemStatus.postgresql.healthy : true,
    },
    {
      name: 'Backend',
      status: systemStatus?.backend.status || 'Connected',
      healthy: systemStatus ? systemStatus.backend.healthy : true,
    },
    {
      name: 'WebSocket',
      status: wsConnected ? 'Connected' : 'Reconnecting',
      healthy: wsConnected,
    },
  ];

  return (
    <section
      aria-label="System Status"
      className="bg-slate-900/90 border border-slate-800 rounded-xl p-4 shadow-lg"
    >
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            System Status
          </span>
        </div>

        <div className="flex flex-wrap items-center gap-3 sm:gap-6">
          {items.map((item) => {
            const isRunningOrConnected =
              item.healthy &&
              (item.status === 'Connected' ||
                item.status === 'Running' ||
                item.status === 'Ready');
            const dotColor = isRunningOrConnected
              ? item.status === 'Running'
                ? 'bg-emerald-400 animate-pulse'
                : 'bg-emerald-400'
              : 'bg-amber-400';

            return (
              <div
                key={item.name}
                className="flex items-center gap-2 bg-slate-950/80 border border-slate-800/80 px-3 py-1.5 rounded-lg"
              >
                <span className="text-xs font-medium text-slate-300">{item.name}</span>
                <span className="flex items-center gap-1.5 text-xs font-semibold text-emerald-400">
                  <span className={`h-2 w-2 rounded-full ${dotColor}`} />
                  {item.status}
                </span>
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
};
