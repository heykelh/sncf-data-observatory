"use client";

import { useEffect, useState, useCallback } from "react";
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
  NO_SERVICE: "Train supprimé",
  SIGNIFICANT_DELAYS: "Retards importants",
  REDUCED_SERVICE: "Service réduit",
  MODIFIED_SERVICE: "Horaires modifiés",
  DETOUR: "Déviation",
  ADDITIONAL_SERVICE: "Train supplémentaire",
  STOP_MOVED: "Changement de quai",
  OTHER_EFFECT: "Perturbation",
  UNKNOWN_EFFECT: "Perturbation",
};

function parseTripId(tripId: string | null, entityId: string) {
  const raw = tripId ?? entityId;
  const upper = raw.toUpperCase();
  let type = "Train"; let icon = "🚆";
  if (upper.includes("INOUI"))                                       { type = "TGV inOui";  icon = "🚄"; }
  else if (upper.includes("TGV"))                                    { type = "TGV";        icon = "🚄"; }
  else if (upper.includes("OUIGO"))                                  { type = "OUIGO";      icon = "🚅"; }
  else if (upper.includes(":CTE:") || upper.includes("INTERCITES")) { type = "Intercités"; icon = "🚂"; }
  else if (upper.includes(":TER:") || upper.includes("TER"))        { type = "TER";        icon = "🚃"; }
  else if (upper.includes("LYRIA"))                                  { type = "Lyria";      icon = "🚄"; }

  const numMatch = raw.match(/OCESN(\d{4,6})/i);
  const numero   = numMatch ? numMatch[1] : "";

  // Regex amélioré — matche tous les formats UIC SNCF
  const uicMatch = raw.match(/:(\d{8}):(\d{8})(?::|$)/);

  return {
    type, icon,
    numero:    numero || "—",
    label:     numero ? `${type} n°${numero}` : type,
    originUic: uicMatch ? uicMatch[1] : null,
    destUic:   uicMatch ? uicMatch[2] : null,
  };
}

function fmtDelay(min: number | null) {
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
  pink:   { neon: "neon-p", dot: "#f472b6", border: "border-pink-500/20"   },
  amber:  { neon: "neon-a", dot: "#fbbf24", border: "border-amber-500/20"  },
  cyan:   { neon: "neon-c", dot: "#22d3ee", border: "border-cyan-500/20"   },
};

function tripColor(delay: number | null, cancelled: boolean): ColorKey {
  if (cancelled)            return "pink";
  if (!delay || delay <= 1) return "green";
  if (delay <= 5)           return "amber";
  if (delay <= 15)          return "violet";
  return "pink";
}

// ── Stat Card ──────────────────────────────────────────────────────────────

function StatCard({ label, value, sub, color, explain, onClick }: {
  label: string; value: string | number; sub?: string;
  color: ColorKey; explain: string; onClick?: () => void;
}) {
  const [tip, setTip] = useState(false);
  const c = COLOR[color];
  return (
    <div
      className={`card border ${c.border} p-5 flex flex-col gap-2 relative ${onClick ? "cursor-pointer hover:bg-white/[0.02] transition-colors" : "cursor-help"}`}
      onMouseEnter={() => setTip(true)}
      onMouseLeave={() => setTip(false)}
      onClick={onClick}
    >
      <span className="text-[10px] font-500 tracking-widest text-white/30 uppercase">{label}</span>
      <span className={`font-700 text-4xl lg:text-5xl leading-none ${c.neon}`}>{value}</span>
      {sub && <span className="text-[11px] text-white/30 font-400 leading-snug mt-1">{sub}</span>}
      {onClick && <span className="text-[9px] text-violet-400/50 font-300 mt-auto">↓ Voir le détail</span>}
      {tip && !onClick && (
        <div className="absolute bottom-full left-0 mb-2 z-30 w-64 bg-[#151525] border border-white/10 rounded-xl p-4 shadow-2xl pointer-events-none">
          <p className="text-xs text-white/60 font-300 leading-relaxed">{explain}</p>
        </div>
      )}
    </div>
  );
}

// ── Ponctualité — 3 blocs ──────────────────────────────────────────────────

function PunctualityBlocks({ kpi }: { kpi: KpiPunctualityRow[] }) {
  if (!kpi.length) return null;

  const hour = new Date().getHours();

  const morning   = kpi.filter(k => { const h = parseInt(String(k.hour).slice(11, 13) || "0"); return h >= 6  && h < 12; });
  const afternoon = kpi.filter(k => { const h = parseInt(String(k.hour).slice(11, 13) || "0"); return h >= 12 && h < 18; });
  const allPeriod = kpi.filter(k => k.punctuality_rate && k.punctuality_rate > 0);

  function avgRate(rows: KpiPunctualityRow[]) {
    const valid = rows.filter(r => r.punctuality_rate && r.punctuality_rate > 0);
    if (!valid.length) return null;
    return Math.round(valid.reduce((a, r) => a + (r.punctuality_rate ?? 0), 0) / valid.length);
  }

  function totalDelayed(rows: KpiPunctualityRow[]) {
    return rows.reduce((a, r) => a + (r.delayed_trips ?? 0), 0);
  }

  const blocks = [
    { label: "Ce matin (6h–12h)",        rate: avgRate(morning),   delayed: totalDelayed(morning),   visible: hour >= 6  },
    { label: "Cet après-midi (12h–18h)", rate: avgRate(afternoon), delayed: totalDelayed(afternoon), visible: hour >= 12 },
    { label: "Ces 6 dernières heures",   rate: avgRate(allPeriod), delayed: totalDelayed(allPeriod), visible: true },
  ].filter(b => b.visible && b.rate !== null);

  if (!blocks.length) return null;

  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
      {blocks.map((b, i) => {
        const col = !b.rate ? "#6b7280" : b.rate >= 90 ? "#10b981" : b.rate >= 80 ? "#fbbf24" : "#f472b6";
        const pct = b.rate ?? 0;
        return (
          <div key={i} className="card p-5">
            <p className="text-[10px] tracking-widest text-white/25 uppercase font-500 mb-3">{b.label}</p>
            <div className="flex items-end gap-3 mb-3">
              <span className="font-700 text-4xl leading-none" style={{ color: col, textShadow: `0 0 10px ${col}` }}>
                {b.rate}%
              </span>
              <span className="text-xs text-white/30 font-300 mb-1">à l&apos;heure</span>
            </div>
            <div className="progress-track mb-2">
              <div className="progress-fill" style={{ width: `${pct}%`, background: col, boxShadow: `0 0 5px ${col}` }} />
            </div>
            <p className="text-xs text-white/25 font-300">
              {b.delayed > 0
                ? `${b.delayed} train${b.delayed > 1 ? "s" : ""} en retard sur la période`
                : "Aucun retard significatif"}
            </p>
            <p className="text-[10px] font-300 mt-2" style={{ color: col }}>
              {pct >= 90 ? "✓ Très bon niveau de service"
               : pct >= 80 ? "~ Service perturbé mais acceptable"
               : "✗ Service fortement perturbé"}
            </p>
          </div>
        );
      })}
    </div>
  );
}

// ── Quality Orb ────────────────────────────────────────────────────────────

function QualityOrb({ score, label }: { score: number; label: string }) {
  const pct  = Math.round(score * 100);
  const col  = pct >= 90 ? "#10b981" : pct >= 70 ? "#fbbf24" : "#f472b6";
  const r    = 28;
  const circ = 2 * Math.PI * r;
  const dash = (pct / 100) * circ;
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

// ── Modale détail train ────────────────────────────────────────────────────

function TripModal({ trip, stops, alerts, onClose }: {
  trip:   LiveFeedResponse["trips"][0];
  stops:  StopsCache;
  alerts: AlertsFeedResponse | null;
  onClose: () => void;
}) {
  const parsed     = parseTripId(trip.trip_id, trip.entity_id);
  const originName = parsed.originUic ? (stops[parsed.originUic]?.name ?? `Code ${parsed.originUic}`) : null;
  const destName   = parsed.destUic   ? (stops[parsed.destUic]?.name   ?? `Code ${parsed.destUic}`)   : null;
  const delay      = fmtDelay(trip.max_delay_minutes);
  const ck         = tripColor(trip.max_delay_minutes, trip.has_cancellation);
  const c          = COLOR[ck];

  const relatedAlerts = (alerts?.alerts ?? []).filter(a =>
    parsed.numero && parsed.numero !== "—" && a.header_text?.includes(parsed.numero)
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4"
         style={{ background: "rgba(0,0,0,0.7)", backdropFilter: "blur(4px)" }}
         onClick={onClose}>
      <div className="w-full max-w-lg card border-violet-500/25 p-6 space-y-5"
           onClick={e => e.stopPropagation()}>

        <div className="flex items-start justify-between">
          <div>
            <p className="font-700 text-xl text-white">{parsed.icon} {parsed.label}</p>
            {originName && destName && (
              <p className="text-sm text-white/50 font-300 mt-1">
                {originName} <span className="text-violet-400">→</span> {destName}
              </p>
            )}
          </div>
          <button onClick={onClose} className="text-white/30 hover:text-white/70 text-xl leading-none mt-1">✕</button>
        </div>

        <div className="divider" />

        <div>
          <p className="text-[10px] tracking-widest text-white/25 uppercase font-500 mb-3">Situation actuelle</p>
          <div className={`flex items-center gap-3 p-4 rounded-xl border ${c.border}`}
               style={{ background: `${c.dot}10` }}>
            <div className="w-3 h-3 rounded-full flex-shrink-0"
                 style={{ background: c.dot, boxShadow: `0 0 8px ${c.dot}` }} />
            <div>
              <p className={`font-600 text-base ${c.neon}`}>
                {trip.has_cancellation ? "🚫 Train supprimé" : delay ?? "✓ À l'heure"}
              </p>
              {trip.affected_stops > 0 && (
                <p className="text-xs text-white/40 font-300 mt-0.5">
                  {trip.affected_stops} arrêt{trip.affected_stops > 1 ? "s" : ""} impacté{trip.affected_stops > 1 ? "s" : ""}
                </p>
              )}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-3">
          {[
            { label: "Type de train", value: parsed.type },
            { label: "N° commercial",  value: parsed.numero !== "—" ? parsed.numero : "Non disponible" },
            { label: "Départ prévu",   value: trip.start_time?.slice(0, 5) ?? "—" },
            { label: "Date",           value: trip.start_date
                ? `${trip.start_date.slice(6,8)}/${trip.start_date.slice(4,6)}/${trip.start_date.slice(0,4)}`
                : "—" },
          ].map(({ label, value }) => (
            <div key={label} className="bg-white/[0.03] rounded-lg p-3">
              <p className="text-[9px] tracking-widest text-white/20 uppercase font-500 mb-1">{label}</p>
              <p className="text-sm font-500 text-white/65">{value}</p>
            </div>
          ))}
        </div>

        {(originName || destName) && (
          <div>
            <p className="text-[10px] tracking-widest text-white/25 uppercase font-500 mb-3">Trajet</p>
            <div className="flex items-center gap-3 bg-white/[0.03] rounded-xl p-4">
              <div className="flex-1 text-center">
                <p className="text-xs text-white/25 font-300 mb-1">Départ</p>
                <p className="font-600 text-sm text-white/70">{originName ?? "—"}</p>
              </div>
              <div className="text-violet-400 text-xl">→</div>
              <div className="flex-1 text-center">
                <p className="text-xs text-white/25 font-300 mb-1">Arrivée</p>
                <p className="font-600 text-sm text-white/70">{destName ?? "—"}</p>
              </div>
            </div>
          </div>
        )}

        {(trip.has_cancellation || (trip.max_delay_minutes && trip.max_delay_minutes > 1)) && (
          <div>
            <p className="text-[10px] tracking-widest text-white/25 uppercase font-500 mb-3">Raison probable</p>
            {relatedAlerts.length > 0 ? (
              relatedAlerts.slice(0, 2).map((a, i) => (
                <div key={i} className="bg-pink-500/5 border border-pink-500/15 rounded-xl p-3 mb-2">
                  <p className="text-xs font-500 text-pink-300 mb-1">
                    {CAUSE_ICON[a.cause] ?? "⚠️"} {CAUSE_FR[a.cause] ?? a.cause} — {EFFECT_FR[a.effect] ?? a.effect}
                  </p>
                  {a.header_text && (
                    <p className="text-xs text-white/40 font-300 leading-relaxed">{a.header_text}</p>
                  )}
                </div>
              ))
            ) : (
              <div className="bg-white/[0.02] border border-white/[0.05] rounded-xl p-3">
                <p className="text-xs text-white/35 font-300 leading-relaxed">
                  {trip.has_cancellation
                    ? "Ce train est supprimé. La cause exacte n'est pas disponible dans le flux temps réel — consultez l'appli SNCF ou les annonces en gare."
                    : `Retard de ${Math.round(trip.max_delay_minutes ?? 0)} min. Cause non précisée — peut être lié à un train précédent en retard, de la densité de trafic, ou un incident sur la voie.`}
                </p>
              </div>
            )}
          </div>
        )}

        <button onClick={onClose}
          className="w-full py-3 text-sm font-500 text-white/40 hover:text-white/70 border border-white/10 rounded-xl transition-colors">
          Fermer
        </button>
      </div>
    </div>
  );
}

// ── Page ───────────────────────────────────────────────────────────────────

export default function LivePage() {
  const [live,        setLive]        = useState<LiveFeedResponse | null>(null);
  const [alerts,      setAlerts]      = useState<AlertsFeedResponse | null>(null);
  const [kpi,         setKpi]         = useState<KpiPunctualityRow[]>([]);
  const [stops,       setStops]       = useState<StopsCache>({});
  const [loading,     setLoading]     = useState(true);
  const [lastUpdate,  setLastUpdate]  = useState("");
  const [tab,         setTab]         = useState<"all" | "delayed" | "cancelled">("all");
  const [selectedTrip, setSelectedTrip] = useState<LiveFeedResponse["trips"][0] | null>(null);

  const alertsRef = useState<HTMLDivElement | null>(null);
  const alertsDivRef = useCallback((node: HTMLDivElement | null) => {
    if (node) (alertsRef as unknown as React.MutableRefObject<HTMLDivElement | null>).current = node;
  }, []);

  const loadStops = useCallback(async (uicCodes: string[]) => {
    const missing = uicCodes.filter(c => c && !stops[c]);
    if (!missing.length) return;
    const batches: string[][] = [];
    for (let i = 0; i < missing.length; i += 50) batches.push(missing.slice(i, i + 50));
    const newStops: StopsCache = {};
    await Promise.all(batches.map(async (batch) => {
      try {
        const base = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
        const res = await fetch(`${base}/stops/resolve?uic_codes=${batch.join(",")}`);
        const data = await res.json();
        Object.assign(newStops, data.stops ?? {});
      } catch { /* silencieux */ }
    }));
    if (Object.keys(newStops).length > 0) {
      setStops(prev => ({ ...prev, ...newStops }));
    }
  }, [stops]);

  const load = useCallback(async () => {
    try {
      const [liveData, alertData, kpiData] = await Promise.all([
        api.tripsLive({ limit: 300 }),
        api.alertsActive({ limit: 200 }),
        api.kpiPunctuality(6),
      ]);
      setLive(liveData);
      setAlerts(alertData);
      setKpi([...kpiData.data].reverse());
      setLastUpdate(new Date().toLocaleTimeString("fr-FR"));
      setLoading(false);

      // Charge les noms de gares pour tous les trips à chaque cycle
      // loadStops ne refetch que les UIC manquants grâce au filtre "missing"
      if (liveData.trips.length > 0) {
        const uics = liveData.trips.flatMap(t => {
          const p = parseTripId(t.trip_id, t.entity_id);
          return [p.originUic, p.destUic].filter(Boolean) as string[];
        });
        const unique = [...new Set(uics)].filter(Boolean);
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

  const sorted = [...(live?.trips ?? [])].sort((a, b) => {
    if (a.has_cancellation !== b.has_cancellation) return a.has_cancellation ? -1 : 1;
    return (b.max_delay_minutes ?? 0) - (a.max_delay_minutes ?? 0);
  });

  const filtered = tab === "delayed"
    ? sorted.filter(t => !t.has_cancellation && (t.max_delay_minutes ?? 0) > 1)
    : tab === "cancelled"
    ? sorted.filter(t => t.has_cancellation)
    : sorted;

  const alertsByCause = (alerts?.alerts ?? []).reduce<Record<string, AlertsFeedResponse["alerts"]>>(
    (acc, a) => { if (!acc[a.cause]) acc[a.cause] = []; acc[a.cause].push(a); return acc; },
    {}
  );

  return (
    <div className="bg-grid min-h-screen">
      {selectedTrip && (
        <TripModal
          trip={selectedTrip}
          stops={stops}
          alerts={alerts}
          onClose={() => setSelectedTrip(null)}
        />
      )}

      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-10 space-y-8">

        {/* En-tête */}
        <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4">
          <div>
            <h1 className="text-4xl sm:text-5xl font-700 tracking-tight text-white leading-none">
              Tableau de bord SNCF
            </h1>
            <p className="text-base text-white/40 font-300 mt-2">
              Suivi en temps réel — TGV · TER · Intercités · OUIGO
            </p>
          </div>
          <div className="flex items-center gap-2 bg-white/[0.03] border border-white/[0.06] rounded-xl px-4 py-2">
            <span className="w-2 h-2 rounded-full bg-emerald-400 animate-blink"
                  style={{ boxShadow: "0 0 8px #10b981" }} />
            <span className="text-sm text-white/50">
              {lastUpdate ? `Actualisé à ${lastUpdate}` : "Chargement..."}
            </span>
          </div>
        </div>

        {/* Bandeau */}
        <div className="bg-violet-500/5 border border-violet-500/15 rounded-xl p-5">
          <p className="text-sm text-white/50 font-300 leading-relaxed">
            <span className="text-violet-300 font-500">Comment ça marche ? </span>
            Connexion toutes les 2 minutes aux données officielles SNCF.
            Les chiffres reflètent la situation{" "}
            <span className="text-white/70 font-500">en ce moment précis</span>.
            Cliquez sur n&apos;importe quel train pour voir son détail complet.
          </p>
        </div>

        {/* Stats cards */}
        <div>
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xs font-500 tracking-widest text-white/25 uppercase">
              En ce moment sur le réseau
            </h2>
            <span className="text-[10px] text-white/20 font-300">
              Snapshot toutes les 2 min · {lastUpdate || "—"}
            </span>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
            <StatCard label="Trains suivis" value={total} color="cyan"
              sub="dans les 60 prochaines min"
              explain={`${total} trains TGV, TER, IC et OUIGO trackés sur les 60 prochaines minutes.`} />
            <StatCard label="À l'heure" value={onTime} color="green"
              sub="retard < 1 min · maintenant"
              explain={`${onTime} trains circulent selon l'horaire prévu, moins d'1 min d'écart.`} />
            <StatCard label="En retard" value={delayed} color="violet"
              sub="retard > 1 min · maintenant"
              explain={`${delayed} trains avec plus d'1 min de retard sur au moins un arrêt.`} />
            <StatCard label="Supprimés" value={cancelled} color="pink"
              sub="trains annulés · maintenant"
              explain={`${cancelled} trains ne circulent pas ou ont une partie du trajet annulée.`} />
            <StatCard label="Ponctualité" value={`${punctRate}%`} color="amber"
              sub={`${onTime} à l'heure sur ${total}`}
              explain={`${punctRate}% des trains sont à l'heure en ce moment. Moyenne historique SNCF : ~87%.`} />
            <StatCard label="Alertes officielles" value={alerts?.active_alerts ?? 0} color="pink"
              sub="cliquer pour le détail"
              explain="Messages officiels SNCF : grèves, travaux, pannes..."
              onClick={() => {
                document.getElementById("alerts-section")?.scrollIntoView({ behavior: "smooth", block: "start" });
              }} />
          </div>
        </div>

        {/* Ponctualité par période */}
        {hasKpi && (
          <div>
            <h2 className="text-xs font-500 tracking-widest text-white/25 uppercase mb-4">
              Ponctualité par période aujourd&apos;hui
            </h2>
            <PunctualityBlocks kpi={kpi} />
          </div>
        )}

        {/* Perturbations */}
        <div id="alerts-section">
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
                  Informations officielles SNCF · regroupées par type
                </p>
              </div>
              <span className="badge text-pink-400 border-pink-500/25">Temps réel</span>
            </div>

            {Object.keys(alertsByCause).length > 0 && (
              <div className="px-5 py-4 border-b border-white/[0.04] flex flex-wrap gap-3">
                {Object.entries(alertsByCause).map(([cause, list]) => (
                  <div key={cause} className="flex items-center gap-1.5 bg-white/[0.03] rounded-lg px-3 py-1.5">
                    <span className="text-sm">{CAUSE_ICON[cause] ?? "⚠️"}</span>
                    <span className="text-xs text-white/50 font-400">{CAUSE_FR[cause] ?? cause}</span>
                    <span className="text-xs font-600 text-pink-400 ml-1">{list.length}</span>
                  </div>
                ))}
              </div>
            )}

            <div className="overflow-y-auto max-h-[460px]">
              {(alerts?.alerts ?? []).length > 0 ? (
                alerts!.alerts.slice(0, 60).map((a) => (
                  <div key={a.entity_id}
                    className="px-5 py-4 border-b border-white/[0.03] hover:bg-white/[0.02] transition-colors">
                    <div className="flex items-start gap-4">
                      <span className="text-2xl flex-shrink-0 mt-0.5">{CAUSE_ICON[a.cause] ?? "⚠️"}</span>
                      <div className="flex-1 min-w-0">
                        <div className="flex flex-wrap items-center gap-2 mb-2">
                          <span className="font-600 text-sm text-white/80">{CAUSE_FR[a.cause] ?? a.cause}</span>
                          <span className="text-white/20">·</span>
                          <span className="text-xs text-white/45 font-300">
                            {EFFECT_FR[a.effect] ?? a.effect.replace(/_/g, " ")}
                          </span>
                          {a.is_cancel && (
                            <span className="badge text-pink-400 border-pink-500/25 text-[8px]">Train supprimé</span>
                          )}
                        </div>
                        {a.header_text && (
                          <p className="text-sm text-white/60 font-300 leading-relaxed">{a.header_text}</p>
                        )}
                        {a.effect === "STOP_MOVED" && !a.header_text && (
                          <p className="text-xs text-white/35 font-300 italic">
                            Un arrêt a été déplacé — vérifiez le quai ou l&apos;arrêt de substitution en gare.
                          </p>
                        )}
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div className="py-12 text-center">
                  <p className="text-3xl mb-2">✅</p>
                  <p className="text-sm text-white/30 font-300">Aucune perturbation signalée</p>
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
              Triée du plus perturbé au plus ponctuel.{" "}
              <span className="text-violet-400/60">Cliquez sur un train pour voir le détail complet.</span>
            </p>
          </div>

          <div className="card">
            <div className="px-4 sm:px-6 py-4 border-b border-white/[0.04]">
              <div className="flex flex-wrap gap-1 bg-white/[0.03] rounded-xl p-1 w-fit">
                {(["all", "delayed", "cancelled"] as const).map(t => {
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

            <div className="px-4 sm:px-6 py-3 border-b border-white/[0.03] bg-white/[0.01] flex flex-wrap gap-4">
              {[
                { col: "#34d399", label: "À l'heure" },
                { col: "#fbbf24", label: "Léger retard (< 5 min)" },
                { col: "#a78bfa", label: "Retard modéré (5–15 min)" },
                { col: "#f472b6", label: "Retard important ou supprimé" },
              ].map(({ col, label }) => (
                <div key={label} className="flex items-center gap-1.5">
                  <div className="w-2 h-2 rounded-full" style={{ background: col, boxShadow: `0 0 4px ${col}` }} />
                  <span className="text-[10px] text-white/35 font-300">{label}</span>
                </div>
              ))}
            </div>

            <div className="hidden sm:grid grid-cols-[14px_140px_1fr_70px_1fr_90px] gap-4 px-6 py-2 border-b border-white/[0.03]">
              {["", "Train", "Départ → Arrivée", "Heure", "Situation", "Arrêts"].map((h, i) => (
                <span key={i} className="text-[9px] tracking-widest text-white/20 font-500 uppercase">{h}</span>
              ))}
            </div>

            <div className="overflow-y-auto max-h-[500px]">
              {filtered.length > 0 ? filtered.slice(0, 200).map(t => {
                const ck         = tripColor(t.max_delay_minutes, t.has_cancellation);
                const c          = COLOR[ck];
                const parsed     = parseTripId(t.trip_id, t.entity_id);
                const delay      = fmtDelay(t.max_delay_minutes);
                const originName = parsed.originUic ? stops[parsed.originUic]?.name : null;
                const destName   = parsed.destUic   ? stops[parsed.destUic]?.name   : null;
                const situation  = t.has_cancellation ? "🚫 Supprimé" : delay ?? "✓ À l'heure";

                return (
                  <div key={t.entity_id}
                    className="grid grid-cols-[14px_1fr_80px] sm:grid-cols-[14px_140px_1fr_70px_1fr_90px] gap-4 items-center px-4 sm:px-6 py-3 border-b border-white/[0.025] hover:bg-white/[0.025] transition-colors cursor-pointer group"
                    onClick={() => setSelectedTrip(t)}>

                    <div className="w-2 h-2 rounded-full flex-shrink-0"
                         style={{ background: c.dot, boxShadow: `0 0 5px ${c.dot}` }} />

                    <div className="min-w-0">
                      <span className="text-sm font-500 text-white/70 group-hover:text-white transition-colors">
                        {parsed.icon} {parsed.label}
                      </span>
                      <div className="sm:hidden mt-0.5">
                        <span className={`text-xs font-600 ${c.neon}`}>{situation}</span>
                      </div>
                    </div>

                    <div className="hidden sm:block min-w-0">
                      {originName && destName ? (
                        <span className="text-xs text-white/50 font-400 truncate block">
                          {originName} <span className="text-violet-400/60">→</span> {destName}
                        </span>
                      ) : originName || destName ? (
                        <span className="text-xs text-white/35 font-300 truncate block">
                          {originName ?? destName}
                        </span>
                      ) : (
                        <span className="text-[10px] text-white/15 font-300 italic">Non résolu</span>
                      )}
                    </div>

                    <span className="hidden sm:block text-xs text-white/35 font-400">
                      {t.start_time?.slice(0, 5) ?? "—"}
                    </span>

                    <span className={`hidden sm:block text-sm font-600 ${c.neon} truncate`}
                          style={{ textShadow: `0 0 8px ${c.dot}` }}>
                      {situation}
                    </span>

                    <div className="hidden sm:flex items-center justify-end gap-2">
                      {t.affected_stops > 0 && (
                        <span className="text-xs text-white/20 font-300">
                          {t.affected_stops} arrêt{t.affected_stops > 1 ? "s" : ""}
                        </span>
                      )}
                      <span className="text-white/15 group-hover:text-violet-400/60 transition-colors text-xs">→</span>
                    </div>
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
                         tab === "cancelled" ? "Aucun train supprimé !" : ""}
                      </p>
                    </>
                  )}
                </div>
              )}
            </div>

            <div className="px-6 py-4 border-t border-white/[0.03] bg-white/[0.01]">
              <p className="text-xs text-white/20 font-300">
                <span className="text-white/40 font-500">{filtered.length} trains</span>
                {" "}· Cliquez sur un train pour le détail · Source GTFS-RT SNCF · Actualisé toutes les 2 min
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
                  <span className="text-sm text-white/30 font-300 w-44 flex-shrink-0">Fraîcheur</span>
                  <div className="flex-1 progress-track">
                    <div className="progress-fill"
                         style={{ width: `${freshPct}%`, background: freshCol, boxShadow: `0 0 5px ${freshCol}` }} />
                  </div>
                  <span className="text-xs font-500 w-20 text-right flex-shrink-0" style={{ color: freshCol }}>
                    {live?.freshness_s
                      ? live.freshness_s < 60  ? "Très fraîche"
                      : live.freshness_s < 180 ? "Fraîche"
                      : "Ancienne"
                      : "—"}
                  </span>
                </div>
                <p className="text-xs text-white/20 font-300">
                  Données vieilles de {live?.freshness_s ? `${Math.round(live.freshness_s)}s` : "—"} ·
                  Dernière MAJ SNCF :{" "}
                  <span className="text-white/40">{live?.fetched_at?.slice(11, 19) ?? "—"}</span>
                </p>
              </div>
              <div className="flex gap-6 justify-center">
                <QualityOrb score={live?.quality_score ?? 0}   label="Fiabilité horaires" />
                <QualityOrb score={alerts?.quality_score ?? 0} label="Fiabilité alertes"  />
              </div>
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}