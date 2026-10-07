import React, { useEffect, useState } from 'react';
import { Pause, Play, RotateCcw, Square } from 'lucide-react';
import { StreamLifecycleState } from '../types/analytics';

interface StreamControlsProps {
  streamState: StreamLifecycleState;
  currentRate: number;
  currentCategory: string;
  onStart: (rate: number, category: string) => Promise<void>;
  onPause: () => Promise<void>;
  onResume: () => Promise<void>;
  onStop: () => Promise<void>;
  isLoading: boolean;
}

const CATEGORIES = [
  'Technology',
  'Big Data',
  'Artificial Intelligence',
  'Data Science',
  'News',
  'Product Reviews',
  'All',
];

export const StreamControls: React.FC<StreamControlsProps> = ({
  streamState,
  currentRate,
  currentCategory,
  onStart,
  onPause,
  onResume,
  onStop,
  isLoading,
}) => {
  const [rate, setRate] = useState<number>(currentRate || 10);
  const [category, setCategory] = useState<string>(currentCategory || 'Technology');

  useEffect(() => {
    setRate(currentRate);
  }, [currentRate]);

  useEffect(() => {
    if (currentCategory) {
      setCategory(currentCategory);
    }
  }, [currentCategory]);

  // Strict state-based enable/disable logic (Requirement 7 Part C & Section 13)
  const canStart = streamState === 'STOPPED' && !isLoading;
  const canPause = streamState === 'RUNNING' && !isLoading;
  const canResume = streamState === 'PAUSED' && !isLoading;
  const canStop = (streamState === 'RUNNING' || streamState === 'PAUSED') && !isLoading;

  const stateBadgeStyles: Record<StreamLifecycleState, string> = {
    STOPPED: 'bg-slate-800 text-slate-300 border-slate-700',
    RUNNING: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
    PAUSED: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
  };

  return (
    <section
      aria-label="Stream Controls"
      className="bg-slate-900/90 border border-slate-800 rounded-xl p-5 shadow-lg"
    >
      <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-5">
        {/* Left: Stream State & Configuration Inputs */}
        <div className="flex flex-wrap items-center gap-5">
          <div>
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400 block mb-1.5">
              Stream State
            </span>
            <div
              className={`inline-flex items-center gap-2 px-3.5 py-2 rounded-lg border text-xs font-bold tracking-wider ${stateBadgeStyles[streamState]}`}
            >
              <span
                className={`h-2 w-2 rounded-full ${
                  streamState === 'RUNNING'
                    ? 'bg-emerald-400 animate-ping'
                    : streamState === 'PAUSED'
                    ? 'bg-amber-400'
                    : 'bg-slate-500'
                }`}
              />
              {streamState}
            </div>
          </div>

          {/* Messages / second */}
          <div className="min-w-[200px]">
            <div className="flex items-center justify-between mb-1.5">
              <label
                htmlFor="rate-input"
                className="text-xs font-semibold uppercase tracking-wider text-slate-400"
              >
                Messages / second
              </label>
              <span className="text-xs font-bold text-indigo-400 tabular-nums">
                {rate} msg/s
              </span>
            </div>
            <div className="flex items-center gap-3">
              <input
                id="rate-input"
                type="range"
                min={1}
                max={50}
                step={1}
                value={rate}
                onChange={(e) => setRate(Number(e.target.value))}
                className="w-32 accent-indigo-500 cursor-pointer"
              />
              <input
                type="number"
                min={1}
                max={100}
                value={rate}
                onChange={(e) =>
                  setRate(Math.max(1, Math.min(100, Number(e.target.value) || 1)))
                }
                aria-label="Messages per second numeric input"
                className="w-16 bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1.5 text-xs font-semibold text-slate-100 focus:outline-none focus:border-indigo-500 tabular-nums"
              />
            </div>
          </div>

          {/* Data Category */}
          <div className="min-w-[210px]">
            <label
              htmlFor="category-select"
              className="text-xs font-semibold uppercase tracking-wider text-slate-400 block mb-1.5"
            >
              Data Category
            </label>
            <select
              id="category-select"
              value={category}
              onChange={(e) => setCategory(e.target.value)}
              className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-xs font-medium text-slate-100 focus:outline-none focus:border-indigo-500"
            >
              {CATEGORIES.map((cat) => (
                <option key={cat} value={cat}>
                  {cat}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Right: START / PAUSE / RESUME / STOP Lifecycle Buttons */}
        <div className="flex flex-wrap items-center gap-3">
          <button
            type="button"
            disabled={!canStart}
            onClick={() => onStart(rate, category)}
            className={`inline-flex items-center gap-2 px-4 py-2.5 rounded-lg text-xs font-bold uppercase tracking-wider transition-all ${
              canStart
                ? 'bg-emerald-600 hover:bg-emerald-500 text-white shadow-lg shadow-emerald-600/25 cursor-pointer'
                : 'bg-slate-800/60 text-slate-500 border border-slate-800 cursor-not-allowed'
            }`}
          >
            <Play className="w-4 h-4 fill-current" />
            START
          </button>

          <button
            type="button"
            disabled={!canPause}
            onClick={onPause}
            className={`inline-flex items-center gap-2 px-4 py-2.5 rounded-lg text-xs font-bold uppercase tracking-wider transition-all ${
              canPause
                ? 'bg-amber-600 hover:bg-amber-500 text-white shadow-lg shadow-amber-600/25 cursor-pointer'
                : 'bg-slate-800/60 text-slate-500 border border-slate-800 cursor-not-allowed'
            }`}
          >
            <Pause className="w-4 h-4" />
            PAUSE
          </button>

          <button
            type="button"
            disabled={!canResume}
            onClick={onResume}
            className={`inline-flex items-center gap-2 px-4 py-2.5 rounded-lg text-xs font-bold uppercase tracking-wider transition-all ${
              canResume
                ? 'bg-indigo-600 hover:bg-indigo-500 text-white shadow-lg shadow-indigo-600/25 cursor-pointer'
                : 'bg-slate-800/60 text-slate-500 border border-slate-800 cursor-not-allowed'
            }`}
          >
            <RotateCcw className="w-4 h-4" />
            RESUME
          </button>

          <button
            type="button"
            disabled={!canStop}
            onClick={onStop}
            className={`inline-flex items-center gap-2 px-4 py-2.5 rounded-lg text-xs font-bold uppercase tracking-wider transition-all ${
              canStop
                ? 'bg-rose-600 hover:bg-rose-500 text-white shadow-lg shadow-rose-600/25 cursor-pointer'
                : 'bg-slate-800/60 text-slate-500 border border-slate-800 cursor-not-allowed'
            }`}
          >
            <Square className="w-4 h-4 fill-current" />
            STOP
          </button>
        </div>
      </div>
    </section>
  );
};
