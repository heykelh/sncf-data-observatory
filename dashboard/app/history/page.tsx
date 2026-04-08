"use client";

import { useEffect, useState, useCallback } from "react";
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, Legend, CartesianGrid,
} from "recharts";
import { api, HistoryRow } from "@/lib/api";

// ── Types ──────────────────────────────────────────────────────────────────

interface TrainTypeConfig {
  key:   string;
  label: string;
  icon:  string;
  color: string;
  desc:  string;
}

// ── Config ─────────────────────────────────────────────────────────────────

const TRAIN_TYPES: TrainTypeConfig[] = [
  { key: "tgv",        label: "TGV",        icon: "🚄", color: "#8b5cf6", desc: "Trains à Grande Vitesse — Paris ↔ Lyon, Bordeaux, Marseille, Lille..." },
  { key: "ter",        label: "TER",        icon: "🚃", color: "#10b981", desc: "Trains Express Régionaux — liaisons régionales dans toute la France" },
  { key: "transilien", label: "Transilien", icon: "🚇", color: "#22d3ee", desc: "Trains de banlieue parisienne — RER et lignes Transilien en Île-de-France" },
  { key: "intercites", label: "Intercités", icon: "🚂", color: "#fbbf24", desc: "Trains Intercités — liaisons longue distance non-TGV" },
];

const YEAR_OPTIONS = [2013, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024];

const MONTHS_FR = ["", "Jan", "Fév", "Mar", "Avr", "Mai", "Jun", "Jul", "Aoû", "Sep", "Oct", "Nov", "Déc"];

const EVENTS = [
  { year: 2018, month: 4,  label: "Grève SNCF 3 mois",      color: "#f472b6" },
  { year: 2019, month: 12, label: "Grève retraites",         color: "#f472b6" },
  { year: 2020, month: 3,  label: "COVID-19 confinement",    color: "#ef4444" },
  { year: 2022, month: 6,  label: "Canicule record",         color: "#f97316" },
  { year: 2023, month: 3,  label: "Grève réforme retraites", color: "#f472b6" },
];

// ── Helpers ────────────────────────────────────────────────────────────────

function fmtPeriod(period: string): string {
  if (!period || !period.includes("-")) return period;
  const [year, month] = period.split("-");
  return `${MONTHS_FR[parseInt(month)] ?? month} ${year}`;
}

function calcStats(data: HistoryRow[]) {
  const rates = data.map(d => d.punctuality_rate).filter((r): r is number => r !== null && r > 0);
  if (!rates.length) return { avg: null, min: null, max: null, trend: null };
  const avg  = rates.reduce((a, b) => a + b, 0) / rates.length;
  const min  = Math.min(...rates);
  const max  = Math.max(...rates);
  const half = Math.floor(rates.length / 2);
  const firstHalf  = rates.slice(0, half).reduce((a, b) => a + b, 0) / (half || 1);
  const secondHalf = rates.slice(half).reduce((a, b) => a + b, 0) / ((rates.length - half) || 1);
  const trend = secondHalf - firstHalf;
  return {
    avg:   Math.round(avg   * 10) / 10,
    min:   Math.round(min   * 10) / 10,
    max:   Math.round(max   * 10) / 10,
    trend: Math.round(trend * 10) / 10,
  };
}

function getColorForRate(rate: number | null): string {
  if (!rate) return "#6b7280";
  if (rate >= 92) return "#10b981";
  if (rate >= 85) return "#fbbf24";
  if (rate >= 75) return "#f97316";
  return "#f472b6";
}

// ── Tooltip ────────────────────────────────────────────────────────────────

function ChartTooltip({ active, payload, label }: {
  active?:  boolean;
  payload?: Array<{ value: number; name: string; color: string; dataKey: string }>;
  label?:   string | number;
}) {
  if (!active || !payload?.length || label == null) return null;

  // label peut être "2020-03" (courbe mensuelle), "Jan" (barres mois) ou 2020 (barres années)
  const labelStr     = String(label);
  const displayLabel = labelStr.includes("-") ? fmtPeriod(labelStr) : labelStr;

  return (
    <div className="bg-[#0d0d1a] border border-white/10 rounded-xl px-4 py-3 text-xs shadow-xl">
      <p className="text-white/50 mb-2 font-500">{displayLabel}</p>
      {payload.map((p, i) =>
        p.value != null ? (
          <div key={i} className="flex items-center gap-2 mb-1">
            <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ background: p.color }} />
            <span className="text-white/40 font-300">{p.name} :</span>
            <span className="font-600" style={{ color: p.color }}>
              {typeof p.value === "number" ? `${p.value.toFixed(1)}%` : p.value}
            </span>
          </div>
        ) : null
      )}
    </div>
  );
}

// ── Mini stat ──────────────────────────────────────────────────────────────

function MiniStat({ label, value, color, sub }: {
  label: string; value: string | null; color: string; sub?: string;
}) {
  return (
    <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-4">
      <span className="text-[9px] tracking-widest text-white/25 uppercase font-500 block mb-2">{label}</span>
      <span className="font-700 text-2xl leading-none" style={{ color, textShadow: `0 0 10px ${color}` }}>
        {value ?? "—"}
      </span>
      {sub && <span className="text-[10px] text-white/20 font-300 block mt-1">{sub}</span>}
    </div>
  );
}

// ── Page ───────────────────────────────────────────────────────────────────

export default function HistoryPage() {
  const [selectedType, setSelectedType] = useState<string>("tgv");
  const [yearFrom,     setYearFrom]     = useState<number>(2015);
  const [data,         setData]         = useState<HistoryRow[]>([]);
  const [allData,      setAllData]      = useState<Record<string, HistoryRow[]>>({});
  const [loading,      setLoading]      = useState(true);
  const [view,         setView]         = useState<"line" | "bar">("line");

  const loadAll = useCallback(async () => {
    try {
      const results = await Promise.all(
        TRAIN_TYPES.map(t => api.kpiHistory(t.key, 2013).then(r => ({ key: t.key, data: r.data })))
      );
      const map: Record<string, HistoryRow[]> = {};
      results.forEach(r => { map[r.key] = r.data; });
      setAllData(map);
      setData((map["tgv"] ?? []).filter(d => d.year >= 2015));
      setLoading(false);
    } catch (err) {
      console.error(err);
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadAll(); }, [loadAll]);

  useEffect(() => {
    const raw = allData[selectedType] ?? [];
    setData(raw.filter(d => d.year >= yearFrom));
  }, [selectedType, yearFrom, allData]);

  if (loading) return (
    <div className="min-h-[80vh] flex items-center justify-center">
      <div className="text-center space-y-4">
        <div className="w-8 h-8 border-2 border-violet-500/30 border-t-violet-400 rounded-full animate-spin mx-auto" />
        <p className="text-lg font-600 text-white/60">Chargement des données historiques...</p>
        <p className="text-sm text-white/20 font-300">10 ans de régularité SNCF · 10 801 enregistrements</p>
      </div>
    </div>
  );

  const cfg     = TRAIN_TYPES.find(t => t.key === selectedType)!;
  const stats   = calcStats(data);
  const hasData = data.length > 0 && data.some(d => d.punctuality_rate != null);

  // Données comparaison toutes lignes
  const compareData = (() => {
    const periods = new Set<string>();
    Object.values(allData).forEach(rows =>
      rows.filter(r => r.year >= yearFrom).forEach(r => periods.add(r.period))
    );
    return [...periods].sort().map(period => {
      const row: Record<string, number | string | null> = { period };
      TRAIN_TYPES.forEach(t => {
        const match = (allData[t.key] ?? []).find(r => r.period === period);
        row[t.key] = match?.punctuality_rate ?? null;
      });
      return row;
    });
  })();

  // Ponctualité moyenne par mois de l'année
  const monthlyAvg = (() => {
    const byMonth: Record<number, number[]> = {};
    data.forEach(d => {
      if (d.punctuality_rate == null) return;
      if (!byMonth[d.month]) byMonth[d.month] = [];
      byMonth[d.month].push(d.punctuality_rate);
    });
    return Array.from({ length: 12 }, (_, i) => {
      const m    = i + 1;
      const vals = byMonth[m] ?? [];
      return {
        month: MONTHS_FR[m],
        avg:   vals.length
          ? Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 10) / 10
          : null,
      };
    });
  })();

  // Ponctualité moyenne par année
  const yearlyAvg = (() => {
    const byYear: Record<number, number[]> = {};
    data.forEach(d => {
      if (d.punctuality_rate == null) return;
      if (!byYear[d.year]) byYear[d.year] = [];
      byYear[d.year].push(d.punctuality_rate);
    });
    return Object.entries(byYear)
      .map(([year, vals]) => ({
        year: parseInt(year),
        avg:  Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 10) / 10,
      }))
      .sort((a, b) => a.year - b.year);
  })();

  const worstYear = yearlyAvg.length ? yearlyAvg.reduce((a, b) => a.avg < b.avg ? a : b) : null;
  const bestYear  = yearlyAvg.length ? yearlyAvg.reduce((a, b) => a.avg > b.avg ? a : b) : null;

  return (
    <div className="bg-grid min-h-screen">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-10 space-y-8">

        {/* En-tête */}
        <div>
          <h1 className="text-4xl sm:text-5xl font-700 tracking-tight text-white leading-none mb-3">
            Historique de régularité
          </h1>
          <p className="text-base text-white/35 font-300 max-w-2xl">
            10 ans de données officielles SNCF — ponctualité mensuelle de 2013 à aujourd&apos;hui.
            Source : Open Data SNCF · Autorité de la Qualité de Service dans les Transports (AQST).
          </p>
        </div>

        {/* Bandeau explicatif */}
        <div className="bg-violet-500/5 border border-violet-500/15 rounded-xl p-5">
          <p className="text-sm text-white/50 font-300 leading-relaxed">
            <span className="text-violet-300 font-500">Comment lire ces données ? </span>
            Le <span className="text-white/70 font-500">taux de régularité</span> est le pourcentage
            de trains arrivés à l&apos;heure à destination — moins de 5 minutes de retard pour les trajets
            de moins d&apos;1h30, moins de 10 minutes pour les trajets de 1h30 à 3h.
            Un train supprimé avant 16h la veille n&apos;est pas comptabilisé comme retardé.
          </p>
        </div>

        {/* Sélecteurs */}
        <div className="flex flex-col sm:flex-row gap-4 flex-wrap">
          {/* Type de train */}
          <div className="flex flex-wrap gap-2">
            {TRAIN_TYPES.map(t => (
              <button key={t.key} onClick={() => setSelectedType(t.key)}
                className={`flex items-center gap-2 px-4 py-2.5 rounded-xl text-sm font-500 transition-all border ${
                  selectedType === t.key ? "" : "border-white/10 text-white/35 hover:text-white/60 hover:border-white/20"
                }`}
                style={selectedType === t.key ? {
                  borderColor: t.color, background: `${t.color}15`, color: "white",
                  textShadow: `0 0 8px ${t.color}`,
                } : {}}>
                <span>{t.icon}</span><span>{t.label}</span>
              </button>
            ))}
          </div>

          <div className="flex items-center gap-3 sm:ml-auto flex-wrap">
            {/* Depuis */}
            <div className="flex items-center gap-2">
              <span className="text-xs text-white/30 font-300">Depuis</span>
              <select value={yearFrom} onChange={e => setYearFrom(parseInt(e.target.value))}
                className="bg-white/[0.05] border border-white/10 rounded-lg px-3 py-2 text-sm text-white/70 font-400 focus:outline-none focus:border-violet-500/40">
                {YEAR_OPTIONS.map(y => (
                  <option key={y} value={y} className="bg-[#0d0d1a]">{y}</option>
                ))}
              </select>
            </div>

            {/* Vue */}
            <div className="flex items-center gap-1 bg-white/[0.03] border border-white/[0.06] rounded-lg p-1">
              {(["line", "bar"] as const).map(v => (
                <button key={v} onClick={() => setView(v)}
                  className={`px-3 py-1.5 rounded-md text-xs font-500 transition-all ${
                    view === v ? "bg-violet-500/20 text-violet-300" : "text-white/30 hover:text-white/60"
                  }`}>
                  {v === "line" ? "📈 Courbe" : "📊 Barres"}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Description type sélectionné */}
        <div className="flex items-center gap-3 px-5 py-3 rounded-xl border"
             style={{ borderColor: `${cfg.color}25`, background: `${cfg.color}08` }}>
          <span className="text-2xl">{cfg.icon}</span>
          <div>
            <span className="font-600 text-sm" style={{ color: cfg.color }}>{cfg.label}</span>
            <span className="text-sm text-white/35 font-300 ml-2">— {cfg.desc}</span>
          </div>
        </div>

        {/* Stats résumé */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <MiniStat
            label="Ponctualité moyenne"
            value={stats.avg ? `${stats.avg}%` : null}
            color={getColorForRate(stats.avg)}
            sub={`Sur ${data.filter(d => d.punctuality_rate).length} mois analysés`}
          />
          <MiniStat
            label="Meilleur mois"
            value={stats.max ? `${stats.max}%` : null}
            color="#10b981"
            sub={data.find(d => d.punctuality_rate === stats.max)
              ? fmtPeriod(data.find(d => d.punctuality_rate === stats.max)!.period)
              : undefined}
          />
          <MiniStat
            label="Pire mois"
            value={stats.min ? `${stats.min}%` : null}
            color="#f472b6"
            sub={data.find(d => d.punctuality_rate === stats.min)
              ? fmtPeriod(data.find(d => d.punctuality_rate === stats.min)!.period)
              : undefined}
          />
          <MiniStat
            label="Tendance"
            value={stats.trend != null
              ? stats.trend > 0 ? `+${stats.trend}%` : `${stats.trend}%`
              : null}
            color={stats.trend != null
              ? stats.trend > 0 ? "#10b981" : stats.trend < -1 ? "#f472b6" : "#fbbf24"
              : "#6b7280"}
            sub="2ème moitié vs 1ère moitié de période"
          />
        </div>

        {/* Graphique principal */}
        <div className="card p-6">
          <div className="mb-5">
            <h2 className="font-600 text-base text-white/70 mb-1">
              {cfg.icon} Ponctualité {view === "line" ? "mensuelle" : "annuelle"} {cfg.label} — depuis {yearFrom}
            </h2>
            <p className="text-xs text-white/25 font-300">
              {view === "line"
                ? "Chaque point = un mois. La ligne pointillée à 90% représente un bon niveau de service. Survolez pour le détail."
                : "Moyenne annuelle de ponctualité. La couleur indique le niveau : vert = excellent, jaune = correct, rose = difficile."}
            </p>
          </div>

          {hasData ? (
            <ResponsiveContainer width="100%" height={280}>
              {view === "line" ? (
                <LineChart data={data} margin={{ top: 20, right: 10, bottom: 0, left: -20 }}>
                  <CartesianGrid stroke="rgba(255,255,255,0.03)" vertical={false} />
                  <XAxis dataKey="period"
                    tickFormatter={v => String(v).slice(0, 7)}
                    tick={{ fontSize: 9, fill: "rgba(255,255,255,0.2)", fontFamily: "'Kanit',sans-serif" }}
                    axisLine={{ stroke: "rgba(139,92,246,0.12)" }} tickLine={false}
                    interval={Math.floor(data.length / 10)} />
                  <YAxis domain={[60, 100]}
                    tick={{ fontSize: 9, fill: "rgba(255,255,255,0.2)", fontFamily: "'Kanit',sans-serif" }}
                    axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} />
                  <Tooltip content={<ChartTooltip />} />
                  <ReferenceLine y={90} stroke="rgba(255,255,255,0.15)" strokeDasharray="4 4"
                    label={{ value: "90% — bon niveau", fill: "rgba(255,255,255,0.2)", fontSize: 9, fontFamily: "'Kanit',sans-serif" }} />
                  {EVENTS.filter(e => e.year >= yearFrom).map((ev, i) => (
                    <ReferenceLine key={i}
                      x={`${ev.year}-${String(ev.month).padStart(2, "0")}`}
                      stroke={ev.color} strokeDasharray="3 3" strokeOpacity={0.5}
                      label={{ value: ev.label, fill: ev.color, fontSize: 8,
                               fontFamily: "'Kanit',sans-serif", angle: -90, position: "insideTopRight" }} />
                  ))}
                  <Line type="monotone" dataKey="punctuality_rate" name="Ponctualité"
                    stroke={cfg.color} strokeWidth={2} dot={false}
                    activeDot={{ r: 4, fill: cfg.color }}
                    style={{ filter: `drop-shadow(0 0 4px ${cfg.color})` }} />
                </LineChart>
              ) : (
                <BarChart data={yearlyAvg} margin={{ top: 10, right: 10, bottom: 0, left: -20 }}>
                  <CartesianGrid stroke="rgba(255,255,255,0.03)" vertical={false} />
                  <XAxis dataKey="year"
                    tick={{ fontSize: 10, fill: "rgba(255,255,255,0.3)", fontFamily: "'Kanit',sans-serif" }}
                    axisLine={false} tickLine={false} />
                  <YAxis domain={[70, 100]}
                    tick={{ fontSize: 9, fill: "rgba(255,255,255,0.2)", fontFamily: "'Kanit',sans-serif" }}
                    axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} />
                  <Tooltip content={<ChartTooltip />} />
                  <ReferenceLine y={90} stroke="rgba(255,255,255,0.15)" strokeDasharray="4 4" />
                  <Bar dataKey="avg" name="Ponctualité annuelle" radius={[4, 4, 0, 0]}>
                    {yearlyAvg.map((entry, index) => (
                      <rect key={`cell-${index}`}
                        fill={getColorForRate(entry.avg)}
                        style={{ filter: entry.avg < 82 ? `drop-shadow(0 0 6px ${getColorForRate(entry.avg)})` : "none" }} />
                    ))}
                  </Bar>
                </BarChart>
              )}
            </ResponsiveContainer>
          ) : (
            <div className="h-64 flex items-center justify-center">
              <p className="text-sm text-white/20 font-300">Aucune donnée disponible pour cette sélection</p>
            </div>
          )}

          {/* Légende événements */}
          {view === "line" && EVENTS.filter(e => e.year >= yearFrom).length > 0 && (
            <div className="mt-4 flex flex-wrap gap-3 border-t border-white/[0.04] pt-4">
              <span className="text-[9px] tracking-widest text-white/20 uppercase font-500">Événements :</span>
              {EVENTS.filter(e => e.year >= yearFrom).map((ev, i) => (
                <div key={i} className="flex items-center gap-1.5">
                  <div className="w-3 h-px" style={{ background: ev.color }} />
                  <span className="text-[10px] text-white/30 font-300">{ev.year} · {ev.label}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Quel mois est le pire */}
        <div className="card p-6">
          <h2 className="font-600 text-base text-white/70 mb-1">
            📅 Quel mois est le moins ponctuel ?
          </h2>
          <p className="text-xs text-white/25 font-300 mb-5">
            Ponctualité moyenne par mois de l&apos;année — toutes années confondues depuis {yearFrom}.
            Utile pour anticiper les périodes à risque avant de voyager.
          </p>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={monthlyAvg} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
              <XAxis dataKey="month"
                tick={{ fontSize: 10, fill: "rgba(255,255,255,0.3)", fontFamily: "'Kanit',sans-serif" }}
                axisLine={false} tickLine={false} />
              <YAxis domain={[70, 100]}
                tick={{ fontSize: 9, fill: "rgba(255,255,255,0.2)", fontFamily: "'Kanit',sans-serif" }}
                axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} />
              <Tooltip content={<ChartTooltip />} />
              <ReferenceLine y={90} stroke="rgba(255,255,255,0.1)" strokeDasharray="4 4" />
              <Bar dataKey="avg" name="Ponctualité" radius={[4, 4, 0, 0]}>
                {monthlyAvg.map((entry, index) => (
                  <rect key={`m-${index}`}
                    fill={entry.avg ? getColorForRate(entry.avg) : "#374151"}
                    style={{ filter: entry.avg && entry.avg < 85 ? `drop-shadow(0 0 5px ${getColorForRate(entry.avg)})` : "none" }} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>

          {/* Insight automatique */}
          {monthlyAvg.some(m => m.avg !== null) && (() => {
            const valid = monthlyAvg.filter((m): m is { month: string; avg: number } => m.avg !== null);
            if (!valid.length) return null;
            const worst = valid.reduce((a, b) => a.avg < b.avg ? a : b);
            const best  = valid.reduce((a, b) => a.avg > b.avg ? a : b);
            return (
              <div className="mt-4 p-4 bg-white/[0.02] rounded-xl border border-white/[0.05]">
                <p className="text-xs text-white/45 font-300 leading-relaxed">
                  💡 Pour les <span className="text-white/70 font-500">{cfg.label}</span>,
                  le mois le plus ponctuel est{" "}
                  <span className="font-500" style={{ color: "#10b981" }}>{best.month} ({best.avg}%)</span>
                  {" "}et le moins ponctuel est{" "}
                  <span className="font-500" style={{ color: "#f472b6" }}>{worst.month} ({worst.avg}%)</span>.
                  {worst.avg < 85
                    ? " Les périodes de grève et les mois d'hiver tirent la ponctualité vers le bas."
                    : " Le réseau reste globalement stable sur l'année."}
                </p>
              </div>
            );
          })()}
        </div>

        {/* Comparaison toutes lignes */}
        <div className="card p-6">
          <h2 className="font-600 text-base text-white/70 mb-1">
            🔍 Comparaison — Qui est le plus à l&apos;heure ?
          </h2>
          <p className="text-xs text-white/25 font-300 mb-5">
            Toutes les catégories de trains sur la même période.
            La ligne en surbrillance correspond à votre sélection actuelle.
          </p>
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={compareData} margin={{ top: 10, right: 10, bottom: 0, left: -20 }}>
              <CartesianGrid stroke="rgba(255,255,255,0.03)" vertical={false} />
              <XAxis dataKey="period"
                tickFormatter={v => String(v).slice(0, 7)}
                tick={{ fontSize: 9, fill: "rgba(255,255,255,0.2)", fontFamily: "'Kanit',sans-serif" }}
                axisLine={false} tickLine={false}
                interval={Math.floor(compareData.length / 8)} />
              <YAxis domain={[60, 100]}
                tick={{ fontSize: 9, fill: "rgba(255,255,255,0.2)", fontFamily: "'Kanit',sans-serif" }}
                axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} />
              <Tooltip content={<ChartTooltip />} />
              <ReferenceLine y={90} stroke="rgba(255,255,255,0.08)" strokeDasharray="4 4" />
              <Legend
                wrapperStyle={{ fontSize: "11px", color: "rgba(255,255,255,0.4)", fontFamily: "'Kanit',sans-serif", paddingTop: "16px" }}
                formatter={value => TRAIN_TYPES.find(t => t.key === value)?.label ?? value}
              />
              {TRAIN_TYPES.map(t => (
                <Line key={t.key} type="monotone" dataKey={t.key} name={t.key}
                  stroke={t.color} strokeWidth={selectedType === t.key ? 2.5 : 1}
                  dot={false} activeDot={{ r: 3 }}
                  strokeOpacity={selectedType === t.key ? 1 : 0.4}
                  style={{ filter: selectedType === t.key ? `drop-shadow(0 0 4px ${t.color})` : "none" }} />
              ))}
            </LineChart>
          </ResponsiveContainer>

          {/* Classement */}
          {(() => {
            const ranking = TRAIN_TYPES.map(t => {
              const rows = (allData[t.key] ?? []).filter(r => r.year >= yearFrom);
              const s    = calcStats(rows);
              return { ...t, avg: s.avg };
            }).filter(t => t.avg !== null).sort((a, b) => (b.avg ?? 0) - (a.avg ?? 0));
            if (!ranking.length) return null;
            return (
              <div className="mt-5 border-t border-white/[0.04] pt-5">
                <p className="text-[10px] tracking-widest text-white/20 uppercase font-500 mb-3">
                  Classement depuis {yearFrom}
                </p>
                <div className="flex flex-wrap gap-3">
                  {ranking.map((t, i) => (
                    <div key={t.key}
                      className="flex items-center gap-2 bg-white/[0.03] rounded-xl px-4 py-2 border border-white/[0.05]">
                      <span className="text-sm font-700 text-white/20">#{i + 1}</span>
                      <span className="text-lg">{t.icon}</span>
                      <span className="text-sm font-500 text-white/60">{t.label}</span>
                      <span className="font-700 text-sm" style={{ color: t.color }}>{t.avg}%</span>
                    </div>
                  ))}
                </div>
              </div>
            );
          })()}
        </div>

        {/* Meilleures et pires années */}
        {(bestYear || worstYear) && (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            {bestYear && (
              <div className="card p-6" style={{ borderColor: "rgba(16,185,129,0.2)" }}>
                <p className="text-xs text-white/25 font-300 mb-1">
                  🏆 Meilleure année pour les {cfg.label}
                </p>
                <p className="font-700 text-3xl neon-g mb-2">{bestYear.year}</p>
                <p className="text-sm text-white/50 font-300">
                  Ponctualité moyenne de{" "}
                  <span className="font-600" style={{ color: "#10b981" }}>{bestYear.avg}%</span>
                  {" "}— la meilleure performance sur la période analysée.
                </p>
              </div>
            )}
            {worstYear && (
              <div className="card p-6" style={{ borderColor: "rgba(244,114,182,0.2)" }}>
                <p className="text-xs text-white/25 font-300 mb-1">
                  📉 Année la plus difficile pour les {cfg.label}
                </p>
                <p className="font-700 text-3xl neon-p mb-2">{worstYear.year}</p>
                <p className="text-sm text-white/50 font-300">
                  Ponctualité moyenne de{" "}
                  <span className="font-600" style={{ color: "#f472b6" }}>{worstYear.avg}%</span>
                  {worstYear.year === 2020 ? " — année COVID-19, service très réduit." :
                   worstYear.year === 2018 ? " — grève des cheminots de 3 mois." :
                   worstYear.year === 2019 ? " — grève contre la réforme des retraites." : "."}
                </p>
              </div>
            )}
          </div>
        )}

        {/* Teaser IA */}
        <div className="bg-violet-500/5 border border-violet-500/20 rounded-xl p-6">
          <div className="flex items-start gap-4">
            <span className="text-3xl flex-shrink-0">🤖</span>
            <div>
              <p className="font-600 text-sm text-violet-300 mb-2">
                Bientôt disponible — Interrogez les données en langage naturel
              </p>
              <p className="text-sm text-white/40 font-300 leading-relaxed">
                &ldquo;En 2018, quels ont été les trains qui ont fait le plus de retard ?&rdquo; ·
                &ldquo;Quelle journée depuis 2016 a eu le plus de suppressions ?&rdquo; ·
                &ldquo;Pourquoi juin 2022 est si mauvais ?&rdquo;
              </p>
              <p className="text-xs text-white/20 font-300 mt-2">
                Agent LangChain + Claude API — prévu en V2 du projet
              </p>
            </div>
          </div>
        </div>

        {/* Source */}
        <div className="text-center py-4">
          <p className="text-xs text-white/15 font-300">
            Source : Open Data SNCF · AQST · Licence ODbL ·{" "}
            <span className="text-violet-400/40">ressources.data.sncf.com</span>
          </p>
        </div>

      </div>
    </div>
  );
}