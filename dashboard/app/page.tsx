"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";
import { api, LiveFeedResponse, AlertsFeedResponse, KpiPunctualityRow } from "@/lib/api";

// ── Types ──────────────────────────────────────────────────────────────────

interface StopInfo { name: string; lat: number; lon: number; }
interface StopsCache { [uic: string]: StopInfo; }

// ── Helpers ────────────────────────────────────────────────────────────────

const CAUSE_FR: Record<string, string> = {
  STRIKE: "Grève", TECHNICAL_PROBLEM: "Panne technique", WEATHER: "Météo défavorable",
  CONSTRUCTION: "Travaux", ACCIDENT: "Accident", OTHER_CAUSE: "Autre cause",
  UNKNOWN_CAUSE: "Cause inconnue", MAINTENANCE: "Maintenance",
  DEMONSTRATION: "Manifestation", POLICE_ACTIVITY: "Intervention police",
  MEDICAL_EMERGENCY: "Urgence médicale",
};

const CAUSE_ICON: Record<string, string> = {
  STRIKE: "✊", TECHNICAL_PROBLEM: "🔧", WEATHER: "🌧️",
  CONSTRUCTION: "🚧", ACCIDENT: "🚨", MEDICAL_EMERGENCY: "🚑",
  POLICE_ACTIVITY: "👮", DEMONSTRATION: "📢",
};

const EFFECT_FR: Record<string, string> = {
  NO_SERVICE: "Train supprimé — ne circule pas",
  SIGNIFICANT_DELAYS: "Retards importants prévus",
  REDUCED_SERVICE: "Service réduit — moins de trains",
  MODIFIED_SERVICE: "Horaires ou itinéraire modifiés",
  DETOUR: "Déviation de parcours",
  ADDITIONAL_SERVICE: "Train supplémentaire ajouté",
  STOP_MOVED: "Changement de quai ou d'arrêt de substitution",
  OTHER_EFFECT: "Perturbation en cours",
  UNKNOWN_EFFECT: "Perturbation signalée",
};

const EFFECT_ICON: Record<string, string> = {
  NO_SERVICE: "🚫", SIGNIFICANT_DELAYS: "⏱️", REDUCED_SERVICE: "📉",
  MODIFIED_SERVICE: "🔄", DETOUR: "↪️", ADDITIONAL_SERVICE: "➕",
  STOP_MOVED: "📍", OTHER_EFFECT: "⚠️", UNKNOWN_EFFECT: "⚠️",
};

function parseTripId(tripId: string | null, entityId: string): {
  type: string; numero: string; label: string; icon: string;
  originUic: string | null; destUic: string | null;
} {
  const raw = tripId ?? entityId;
  const upper = raw.toUpperCase();
  let type = "Train"; let icon = "🚆";
  if (upper.includes("INOUI"))                               { type = "TGV inOui";  icon = "🚄"; }
  else if (upper.includes("TGV"))                            { type = "TGV";        icon = "🚄"; }
  else if (upper.includes("OUIGO"))                          { type = "OUIGO";      icon = "🚅"; }
  else if (upper.includes(":CTE:") || upper.includes("INTERCITES")) { type = "Intercités"; icon = "🚂"; }
  else if (upper.includes(":TER:") || upper.includes("TER"))        { type = "TER";        icon = "🚃"; }
  else if (upper.includes("LYRIA"))                          { type = "Lyria";      icon = "🚄"; }
  const numMatch = raw.match(/OCESN(\d{4,6})/i);
  const numero = numMatch ? numMatch[1] : raw.slice(5, 11).replace(/\D/g, "");
  const uicMatch = raw.match(/::(\d{8}):(\d{8}):/);
  const originUic = uicMatch ? uicMatch[1] : null;
  const destUic   = uicMatch ? uicMatch[2] : null;
  return { type, icon, numero: numero || "—", label: numero ? `${type} n°${numero}` : type, originUic, destUic };
}

function fmtDelay(min: number | null): string | null {
  if (!min || min <= 0) return null;
  if (min < 2)  return "Légèrement en retard";
  if (min < 10) return `${Math.round(min)} min de retard`;
  if (min < 30) return `${Math.round(min)} min de retard ⚠️`;
  return `${Math.round(min)} min de retard 🚨`;
}

type ColorKey = "violet" | "green" | "pink" | "amber" | "cyan";
const COLOR: Record<ColorKey, { neon: string; dot: string; border: string }> = {
  violet: { neon: "neon-v", dot: "#a78bfa", border: "border-violet-500/20" },
  green:  { neon: "neon-g", dot: "#34d399", border: "border-emerald-500/20" },
  pink:   { neon: "neon-p", dot: "#f472b6", border: "border-pink-500/20" },
  amber:  { neon: "neon-a", dot: "#fbbf24", border: "border-amber-500/20" },
  cyan:   { neon: "neon-c", dot: "#22d3ee", border: "border-cyan-500/20" },
};

function tripColor(delay: number | null, cancelled: boolean): ColorKey {
  if (cancelled) return "pink";
  if (!delay || delay <= 1) return "green";
  if (delay <= 5)  return "amber";
  if (delay <= 15) return "violet";
  return "pink";
}

// ── Composants ─────────────────────────────────────────────────────────────

function StatCard({ label, value, sub, color, explain }: {
  label: string; value: string | number; sub?: string;
  color: ColorKey; explain: string;
}) {
  const [tip, setTip] = useState(false);
  const c = COLOR[color];
  return (
    <div
      className={`card border ${c.border} p-5 flex flex-col gap-2 cursor-help relative`}
      onMouseEnter={() => setTip(true)}
      onMouseLeave={() => setTip(false)}
    >
      <span className="text-[10px] font-500 tracking-widest text-white/30 uppercase">{label}</span>
      <span className={`font-700 text-4xl lg:text-5xl leading-none ${c.neon}`}>{value}</span>
      {sub && <span className="text-[11px] text-white/30 font-400 leading-snug mt-1">{sub}</span>}
      {tip && (
        <div className="absolute bottom-full left-0 mb-2 z-30 w-64 bg-[#151525] border border-white/10 rounded-xl p-4 shadow-2xl pointer-events-none">
          <p className="text-xs text-white/60 font-300 leading-relaxed">{explain}</p>
        </div>
      )}
    </div>
  );
}

function QualityOrb({ score, label }: { score: number; label: string }) {
  const pct = Math.round(score * 100);
  const col = pct >= 90 ? "#10b981" : pct >= 70 ? "#fbbf24" : "#f472b6";
  const r = 28, circ = 2 * Math.PI * r, dash = (pct / 100) * circ;
  return (
    <div className="flex flex-col items-center gap-2">
      <div className="relative w-16 h-16">
        <svg className="w-full h-full -rotate-90" viewBox="0 0 64 64">
          <circle cx="32" cy="32" r={r} fill="none" stroke="rgba(255,255,255,0.05)" strokeWidth="3" />
          <circle cx="32" cy="32" r={r} fill="none" stroke={col} strokeWidth="3"
            strokeDasharray={`${dash} ${circ}`} strokeLinecap="butt"
            style={{ filter: `drop-shadow(0 0 4px ${col})`, transition: "stroke-dasharray 1s ease" }} />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="font-700 text-sm" style={{ color: col, textShadow: `0 0 8px ${col}` }}>{pct}%</span>
        </div>
      </div>
      <span className="text-[9px] tracking-widest text-white/25 uppercase font-500 text-center leading-tight">{label}</span>
    </div>
  );
}

function ChartTooltip({ active, payload, label }: {
  active?: boolean;
  payload?: Array<{ value: number; name: string; color: string }>;
  label?: string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-[#0d0d1a] border border-white/10 rounded-xl px-3 py-2 text-xs">
      <p className="text-white/30 mb-1 font-300">À {String(label).slice(11, 16)}</p>
      {payload.map((p, i) => (
        <p key={i} className="font-500" style={{ color: p.color }}>
          {p.name} : {typeof p.value === "number" ? p.value.toFixed(1) : p.value}
          {p.name === "Ponctualité" ? "%" : " trains"}
        </p>
      ))}
    </div>
  );
}

// ── Page ───────────────────────────────────────────────────────────────────

export default function LivePage() {
  const [live, setLive]       = useState<LiveFeedResponse | null>(null);
  const [alerts, setAlerts]   = useState<AlertsFeedResponse | null>(null);
  const [kpi, setKpi]         = useState<KpiPunctualityRow[]>([]);
  const [stops, setStops]     = useState<StopsCache>({});
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState("");
  const [tab, setTab]         = useState<"all" | "delayed" | "cancelled">("all");
  const stopsLoadedRef        = useRef(false);

  const loadStops = useCallback(async (uicCodes: string[]) => {
    const missing = uicCodes.filter(c => c && !stops[c]);
    if (!missing.length) return;
    const batches: string[][] = [];
    for (let i = 0; i < missing.length; i += 50) batches.push(missing.slice(i, i + 50));
    const newStops: StopsCache = {};
    await Promise.all(batches.map(async (batch) => {
      try {
        const res = await fetch(`/api/stops/resolve?uic_codes=${batch.join(",")}`);
        const data = await res.json();
        Object.assign(newStops, data.stops ?? {});
      } catch { /* silencieux */ }
    }));
    if (Object.keys(newStops).length > 0) setStops(prev => ({ ...prev, ...newStops }));
  }, [stops]);

  const load = useCallback(async () => {
    try {
      const [liveData, alertData, kpiData] = await Promise.all([
        api.tripsLive({ limit: 300 }),
        api.alertsActive({ limit: 150 }),
        api.kpiPunctuality(6),
      ]);
      setLive(liveData);
      setAlerts(alertData);
      setKpi([...kpiData.data].reverse());
      setLastUpdate(new Date().toLocaleTimeString("fr-FR"));
      setLoading(false);
      if (!stopsLoadedRef.current && liveData.trips.length > 0) {
        stopsLoadedRef.current = true;
        const uics = liveData.trips.flatMap(t => {
          const p = parseTripId(t.trip_id, t.entity_id);
          return [p.originUic, p.destUic].filter(Boolean) as string[];
        });
        const unique = [...new Set(uics)];
        if (unique.length > 0) loadStops(unique);
      }
    } catch (err) {
      console.error(err);
      setLoading(false);
    }
  }, [loadStops]);

  useEffect(() => {
    load();
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, [load]);

  if (loading) return (
    <div className="min-h-[80vh] flex items-center justify-center">
      <div className="text-center space-y-4">
        <div className="w-8 h-8 border-2 border-violet-500/30 border-t-violet-400 rounded-full animate-spin mx-auto" />
        <p className="text-lg font-600 text-white/60">Connexion au réseau SNCF...</p>
        <p className="text-sm text-white/20 font-300">Récupération des données en temps réel</p>
      </div>
    </div>
  );

  const total     = live?.total_trips ?? 0;
  const delayed   = live?.delayed_trips ?? 0;
  const cancelled = live?.cancelled_trips ?? 0;
  const onTime    = live?.on_time_trips ?? 0;
  const punctRate = total > 0 ? Math.round((onTime / total) * 100) : 0;
  const freshPct  = live?.freshness_s ? Math.max(0, 100 - (live.freshness_s / 300) * 100) : 100;
  const freshCol  = freshPct >= 80 ? "#10b981" : freshPct >= 50 ? "#fbbf24" : "#f472b6";
  const hasKpi    = kpi.length > 0 && kpi.some(k => (k.total_trips ?? 0) > 0);

  const latestKpi = kpi[kpi.length - 1];
  const kpiSummary = latestKpi?.punctuality_rate
    ? `Sur la dernière heure, ${latestKpi.punctuality_rate.toFixed(0)}% des trains étaient à l'heure`
      + (latestKpi.delayed_trips ? ` et ${latestKpi.delayed_trips} trains étaient en retard.` : ".")
    : null;

  const sorted = [...(live?.trips ?? [])].sort((a, b) => {
    if (a.has_cancellation !== b.has_cancellation) return a.has_cancellation ? -1 : 1;
    return (b.max_delay_minutes ?? 0) - (a.max_delay_minutes ?? 0);
  });

  const filtered = tab === "delayed"
    ? sorted.filter(t => !t.has_cancellation && (t.max_delay_minutes ?? 0) > 1)
    : tab === "cancelled"
    ? sorted.filter(t => t.has_cancellation)
    : sorted;

  return (
    <div className="bg-grid min-h-screen">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-10 space-y-8">

        {/* En-tête */}
        <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4">
          <div>
            <h1 className="text-4xl sm:text-5xl font-700 tracking-tight text-white leading-none">
              Tableau de bord SNCF
            </h1>
            <p className="text-base text-white/40 font-300 mt-2">
              Suivi en temps réel de tous les trains français — TGV, TER, Intercités, OUIGO
            </p>
          </div>
          <div className="flex items-center gap-2 bg-white/[0.03] border border-white/[0.06] rounded-xl px-4 py-2">
            <span className="w-2 h-2 rounded-full bg-emerald-400 animate-blink"
                  style={{ boxShadow: "0 0 8px #10b981" }} />
            <span className="text-sm text-white/50 font-400">
              {lastUpdate ? `Actualisé à ${lastUpdate}` : "Chargement..."}
            </span>
          </div>
        </div>

        {/* Bandeau explicatif */}
        <div className="bg-violet-500/5 border border-violet-500/15 rounded-xl p-5">
          <p className="text-sm text-white/50 font-300 leading-relaxed">
            <span className="text-violet-300 font-500">Comment ça marche ? </span>
            Ce tableau de bord se connecte toutes les 2 minutes aux données officielles de la SNCF
            pour vous montrer l&apos;état exact du réseau à l&apos;instant T.
            Les chiffres ci-dessous reflètent la situation <span className="text-white/70 font-500">en ce moment précis</span>,
            pas une moyenne de la journée. Survolez chaque chiffre pour l&apos;explication détaillée.
          </p>
        </div>

        {/* Stat cards */}
        <div>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xs font-500 tracking-widest text-white/25 uppercase">
              En ce moment sur le réseau
            </h2>
            <span className="text-[10px] text-white/20 font-300">
              Snapshot mis à jour toutes les 2 min · {lastUpdate || "—"}
            </span>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
            <StatCard
              label="Trains suivis"
              value={total}
              color="cyan"
              sub="dans les 60 prochaines min"
              explain={`${total} trains TGV, TER, Intercités et OUIGO sont trackés par la SNCF sur les 60 prochaines minutes. Ce chiffre varie selon l'heure — plus de trains en journée qu'à 2h du matin.`}
            />
            <StatCard
              label="À l'heure"
              value={onTime}
              color="green"
              sub="retard < 1 min · snapshot actuel"
              explain={`${onTime} trains circulent selon leur horaire prévu avec moins d'1 minute d'écart. C'est la situation à cet instant précis, pas une moyenne de la journée.`}
            />
            <StatCard
              label="En retard"
              value={delayed}
              color="violet"
              sub="retard > 1 min · snapshot actuel"
              explain={`${delayed} trains accusent plus d'1 minute de retard sur au moins un arrêt. La cause peut être un incident en amont, de la densité de trafic ou des travaux.`}
            />
            <StatCard
              label="Supprimés"
              value={cancelled}
              color="pink"
              sub="trains annulés · snapshot actuel"
              explain={`${cancelled} trains ne circuleront pas ou ont une partie de leur trajet annulée. Les voyageurs sont généralement redirigés sur un train suivant.`}
            />
            <StatCard
              label="Ponctualité"
              value={`${punctRate}%`}
              color="amber"
              sub={`${onTime} à l'heure sur ${total} trains`}
              explain={`${punctRate}% des trains suivis en ce moment sont à l'heure. C'est la ponctualité instantanée du snapshot actuel — pas la moyenne de la journée. La moyenne historique SNCF est d'environ 87% sur 10 ans.`}
            />
            <StatCard
              label="Alertes officielles"
              value={alerts?.active_alerts ?? 0}
              color="pink"
              sub="messages SNCF en cours"
              explain={`${alerts?.active_alerts ?? 0} messages de perturbation publiés officiellement par la SNCF : grèves, travaux programmés, pannes, incidents météo. Certaines alertes durent plusieurs semaines (travaux de longue durée).`}
            />
          </div>
        </div>

        {/* Graphique */}
        {hasKpi && (
          <div>
            <h2 className="text-xs font-500 tracking-widest text-white/25 uppercase mb-4">
              Ponctualité sur les 6 dernières heures
            </h2>
            <div className="card p-6">
              {kpiSummary && (
                <div className="mb-5 flex items-start gap-3 bg-white/[0.02] rounded-xl p-4">
                  <span className="text-xl flex-shrink-0">📊</span>
                  <div>
                    <p className="text-white/70 font-500 text-sm">{kpiSummary}</p>
                    <p className="text-white/25 text-xs font-300 mt-1">
                      Courbe violette = % de trains à l&apos;heure (plus c&apos;est haut, mieux c&apos;est) ·
                      Courbe rose = nombre de trains en retard
                    </p>
                  </div>
                </div>
              )}
              <ResponsiveContainer width="100%" height={180}>
                <AreaChart data={kpi} margin={{ top: 4, right: 4, bottom: 0, left: -28 }}>
                  <defs>
                    <linearGradient id="gv" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#8b5cf6" stopOpacity={0.35} />
                      <stop offset="100%" stopColor="#8b5cf6" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="gp" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#f472b6" stopOpacity={0.2} />
                      <stop offset="100%" stopColor="#f472b6" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <XAxis dataKey="hour" tickFormatter={v => String(v).slice(11, 16)}
                    tick={{ fontSize: 9, fill: "rgba(255,255,255,0.2)", fontFamily: "'Kanit',sans-serif" }}
                    axisLine={{ stroke: "rgba(139,92,246,0.12)" }} tickLine={false} />
                  <YAxis domain={[0, 110]}
                    tick={{ fontSize: 9, fill: "rgba(255,255,255,0.2)", fontFamily: "'Kanit',sans-serif" }}
                    axisLine={false} tickLine={false} />
                  <Tooltip content={<ChartTooltip />} />
                  <Area type="monotone" dataKey="punctuality_rate" name="Ponctualité"
                    stroke="#8b5cf6" strokeWidth={2} fill="url(#gv)"
                    style={{ filter: "drop-shadow(0 0 4px #8b5cf6)" }} />
                  <Area type="monotone" dataKey="delayed_trips" name="Retards"
                    stroke="#f472b6" strokeWidth={1.5} fill="url(#gp)"
                    style={{ filter: "drop-shadow(0 0 3px #f472b6)" }} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}

        {/* Alertes */}
        <div>
          <h2 className="text-xs font-500 tracking-widest text-white/25 uppercase mb-4">
            Perturbations actives sur le réseau
          </h2>
          <div className="card card-pink">
            <div className="px-5 py-4 border-b border-white/[0.04] flex items-center justify-between">
              <div>
                <p className="font-600 text-white/70 text-base">
                  {alerts?.active_alerts ?? 0} perturbation{(alerts?.active_alerts ?? 0) > 1 ? "s" : ""} en cours
                </p>
                <p className="text-xs text-white/25 font-300 mt-0.5">
                  Informations officielles publiées par la SNCF en temps réel
                </p>
              </div>
              <span className="badge text-pink-400 border-pink-500/25">Temps réel</span>
            </div>
            <div className="overflow-y-auto max-h-[400px]">
              {(alerts?.alerts ?? []).length > 0 ? alerts!.alerts.slice(0, 40).map((a) => (
                <div key={a.entity_id}
                  className="px-5 py-4 border-b border-white/[0.03] hover:bg-white/[0.02] transition-colors">
                  <div className="flex items-start gap-4">
                    <span className="text-2xl flex-shrink-0 mt-0.5">{CAUSE_ICON[a.cause] ?? "⚠️"}</span>
                    <div className="flex-1 min-w-0">
                      <div className="flex flex-wrap items-center gap-2 mb-2">
                        <span className="font-600 text-sm text-white/80">{CAUSE_FR[a.cause] ?? a.cause}</span>
                        <span className="text-white/20">·</span>
                        <span className="text-xs text-white/45 font-300 flex items-center gap-1">
                          <span>{EFFECT_ICON[a.effect] ?? "⚠️"}</span>
                          <span>{EFFECT_FR[a.effect] ?? a.effect.replace(/_/g, " ")}</span>
                        </span>
                        {a.is_cancel && (
                          <span className="badge text-pink-400 border-pink-500/25 text-[8px]">Train supprimé</span>
                        )}
                      </div>
                      {a.header_text && (
                        <p className="text-sm text-white/55 font-300 leading-relaxed">{a.header_text}</p>
                      )}
                      {a.effect === "STOP_MOVED" && !a.header_text && (
                        <p className="text-xs text-white/35 font-300 italic">
                          Un arrêt a été déplacé — vérifiez le quai ou l&apos;arrêt de substitution en gare.
                        </p>
                      )}
                    </div>
                  </div>
                </div>
              )) : (
                <div className="py-12 text-center">
                  <p className="text-3xl mb-2">✅</p>
                  <p className="text-sm text-white/30 font-300">Aucune perturbation signalée actuellement</p>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Feed trains */}
        <div>
          <div className="mb-4">
            <h2 className="text-xs font-500 tracking-widest text-white/25 uppercase">
              État de chaque train en ce moment
            </h2>
            <p className="text-sm text-white/30 font-300 mt-1">
              Liste en temps réel triée du plus perturbé au plus ponctuel — actualisée toutes les 2 minutes.
            </p>
          </div>
          <div className="card">
            {/* Onglets */}
            <div className="px-4 sm:px-6 py-4 border-b border-white/[0.04]">
              <div className="flex flex-wrap gap-1 bg-white/[0.03] rounded-xl p-1 w-fit">
                {(["all", "delayed", "cancelled"] as const).map((t) => {
                  const labels = {
                    all:       `Tous (${total})`,
                    delayed:   `En retard (${delayed})`,
                    cancelled: `Supprimés (${cancelled})`,
                  };
                  return (
                    <button key={t} onClick={() => setTab(t)}
                      className={`px-3 sm:px-4 py-2 rounded-lg text-xs sm:text-sm font-500 transition-all ${
                        tab === t ? "bg-violet-500/20 text-violet-300" : "text-white/30 hover:text-white/60"
                      }`}>
                      {labels[t]}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Légende */}
            <div className="px-4 sm:px-6 py-3 border-b border-white/[0.03] bg-white/[0.01] flex flex-wrap gap-4">
              {[
                { col: "#34d399", label: "À l'heure" },
                { col: "#fbbf24", label: "Léger retard (< 5 min)" },
                { col: "#a78bfa", label: "Retard modéré (5–15 min)" },
                { col: "#f472b6", label: "Retard important ou supprimé" },
              ].map(({ col, label }) => (
                <div key={label} className="flex items-center gap-1.5">
                  <div className="w-2 h-2 rounded-full flex-shrink-0"
                       style={{ background: col, boxShadow: `0 0 4px ${col}` }} />
                  <span className="text-[10px] text-white/35 font-300">{label}</span>
                </div>
              ))}
            </div>

            {/* En-têtes */}
            <div className="hidden sm:grid grid-cols-[14px_1fr_1fr_80px_1fr_90px] gap-4 px-6 py-2 border-b border-white/[0.03]">
              {["", "Train", "Gares (départ → arrivée)", "Heure", "Situation actuelle", "Arrêts impactés"].map((h, i) => (
                <span key={i} className="text-[9px] tracking-widest text-white/20 font-500 uppercase">{h}</span>
              ))}
            </div>

            {/* Lignes */}
            <div className="overflow-y-auto max-h-[500px]">
              {filtered.length > 0 ? filtered.slice(0, 200).map((t) => {
                const ck     = tripColor(t.max_delay_minutes, t.has_cancellation);
                const c      = COLOR[ck];
                const parsed = parseTripId(t.trip_id, t.entity_id);
                const delay  = fmtDelay(t.max_delay_minutes);
                const originName = parsed.originUic ? stops[parsed.originUic]?.name : null;
                const destName   = parsed.destUic   ? stops[parsed.destUic]?.name   : null;
                const situation  = t.has_cancellation ? "🚫 Train supprimé" : delay ?? "✓ À l'heure";

                return (
                  <div key={t.entity_id}
                    className="grid grid-cols-[14px_1fr_80px] sm:grid-cols-[14px_1fr_1fr_80px_1fr_90px] gap-4 items-center px-4 sm:px-6 py-3 border-b border-white/[0.025] hover:bg-white/[0.015] transition-colors">
                    <div className="w-2 h-2 rounded-full flex-shrink-0"
                         style={{ background: c.dot, boxShadow: `0 0 5px ${c.dot}` }} />
                    <div className="min-w-0">
                      <span className="text-sm font-500 text-white/70">{parsed.icon} {parsed.label}</span>
                      <div className="sm:hidden mt-0.5">
                        <span className={`text-xs font-600 ${c.neon}`}>{situation}</span>
                      </div>
                    </div>
                    <div className="hidden sm:flex items-center gap-1 min-w-0">
                      {originName || destName ? (
                        <span className="text-xs text-white/45 font-300 truncate">
                          {originName ?? "—"} → {destName ?? "—"}
                        </span>
                      ) : (
                        <span className="text-[10px] text-white/15 font-300 italic">
                          {t.route_id ? `Ligne ${t.route_id.split(":").pop()?.slice(0, 15)}` : "Chargement..."}
                        </span>
                      )}
                    </div>
                    <span className="hidden sm:block text-xs text-white/35 font-400">
                      {t.start_time?.slice(0, 5) ?? "—"}
                    </span>
                    <span className={`hidden sm:block text-sm font-600 ${c.neon} truncate`}
                          style={{ textShadow: `0 0 8px ${c.dot}` }}>
                      {situation}
                    </span>
                    <span className="hidden sm:block text-xs text-white/20 font-300 text-right">
                      {t.affected_stops > 0 ? `${t.affected_stops} arrêt${t.affected_stops > 1 ? "s" : ""}` : ""}
                    </span>
                  </div>
                );
              }) : (
                <div className="py-16 text-center space-y-2">
                  {total === 0 ? (
                    <>
                      <div className="w-6 h-6 border-2 border-violet-500/30 border-t-violet-400 rounded-full animate-spin mx-auto" />
                      <p className="text-sm text-white/20 font-300">En attente du premier cycle GTFS-RT...</p>
                    </>
                  ) : (
                    <>
                      <p className="text-3xl">🎉</p>
                      <p className="text-sm text-white/40">
                        {tab === "delayed" ? "Aucun train en retard !" :
                         tab === "cancelled" ? "Aucun train supprimé !" : "Aucun résultat"}
                      </p>
                    </>
                  )}
                </div>
              )}
            </div>

            {/* Footer */}
            <div className="px-6 py-4 border-t border-white/[0.03] bg-white/[0.01]">
              <p className="text-xs text-white/20 font-300">
                <span className="text-white/40 font-500">{filtered.length} trains affichés</span>
                {" "}— Source : flux officiel GTFS-RT SNCF, actualisé toutes les 2 min ·{" "}
                <span className="text-violet-400/50">transport.data.gouv.fr</span> · Licence ODbL
              </p>
            </div>
          </div>
        </div>

        {/* Fiabilité */}
        <div>
          <h2 className="text-xs font-500 tracking-widest text-white/25 uppercase mb-4">
            Fiabilité des données
          </h2>
          <div className="card p-5">
            <div className="flex flex-col sm:flex-row sm:items-center gap-6">
              <div className="flex-1 space-y-3">
                <div className="flex items-center gap-3">
                  <span className="text-sm text-white/30 font-300 w-44 flex-shrink-0">Fraîcheur des données</span>
                  <div className="flex-1 progress-track">
                    <div className="progress-fill"
                         style={{ width: `${freshPct}%`, background: freshCol, boxShadow: `0 0 5px ${freshCol}` }} />
                  </div>
                  <span className="text-xs font-500 w-20 text-right flex-shrink-0" style={{ color: freshCol }}>
                    {live?.freshness_s
                      ? live.freshness_s < 60 ? "Très fraîche"
                      : live.freshness_s < 180 ? "Fraîche" : "Ancienne"
                      : "—"}
                  </span>
                </div>
                <p className="text-xs text-white/20 font-300 leading-relaxed">
                  Les données ont {live?.freshness_s ? `${Math.round(live.freshness_s)} secondes` : "—"}.
                  En dessous de 5 minutes c&apos;est excellent.
                  Dernière mise à jour côté SNCF : <span className="text-white/40">{live?.fetched_at?.slice(11, 19) ?? "—"}</span>
                </p>
              </div>
              <div className="flex gap-6 justify-center">
                <QualityOrb score={live?.quality_score ?? 0} label="Fiabilité horaires" />
                <QualityOrb score={alerts?.quality_score ?? 0} label="Fiabilité alertes" />
              </div>
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}