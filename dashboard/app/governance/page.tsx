"use client";

import { useEffect, useState, useCallback } from "react";
import { api, CatalogEntry, QualitySource } from "@/lib/api";

// ── Helpers ────────────────────────────────────────────────────────────────

const STATUS_CONFIG = {
  healthy:  { label: "Opérationnel",  color: "#10b981", bg: "bg-emerald-500/10", border: "border-emerald-500/20", icon: "✓" },
  degraded: { label: "Dégradé",       color: "#fbbf24", bg: "bg-amber-500/10",   border: "border-amber-500/20",   icon: "⚠" },
  down:     { label: "Indisponible",  color: "#f472b6", bg: "bg-pink-500/10",    border: "border-pink-500/20",    icon: "✗" },
  unknown:  { label: "Inconnu",       color: "#6b7280", bg: "bg-white/5",        border: "border-white/10",       icon: "?" },
};

const FEED_TYPE_FR: Record<string, string> = {
  trip_updates:          "Temps réel — retards et suppressions",
  service_alerts:        "Temps réel — perturbations et incidents",
  regularity_historical: "Historique mensuel de régularité",
};

const CHECK_FR: Record<string, { label: string; explain: string; icon: string }> = {
  freshness:      { label: "Fraîcheur",   icon: "⏱️", explain: "Les données ont moins de 5 minutes. En dessous c'est excellent, au-delà c'est un signe de problème côté SNCF." },
  availability:   { label: "Disponibilité", icon: "📡", explain: "Le flux SNCF répond bien (HTTP 200). Si ce contrôle échoue, la source est inaccessible." },
  completeness:   { label: "Complétude",  icon: "📊", explain: "Le flux contient au moins 100 entités. Un flux vide ou quasi-vide signale une anomalie côté SNCF." },
  parse_validity: { label: "Validité",    icon: "✅", explain: "Moins de 5% d'erreurs lors de la lecture des données. Au-delà, le format a peut-être changé." },
};

const ROLE_FR = [
  {
    role: "Data Owner",
    who:  "SNCF Voyageurs",
    what: "Producteur des données sources. Responsable de la qualité et de la disponibilité des flux GTFS-RT publiés sur transport.data.gouv.fr.",
    icon: "🏢",
  },
  {
    role: "Data Steward",
    who:  "Michel DUPONT",
    what: "Responsable du pipeline d'ingestion, de la gouvernance, de la documentation et de la qualité des données dans l'observatory.",
    icon: "👨‍💻",
  },
  {
    role: "Data Consumer",
    who:  "Visiteurs du dashboard",
    what: "Utilisateurs finaux qui consultent les KPIs et alertes en temps réel via le dashboard public.",
    icon: "👥",
  },
];

const SLA_ITEMS = [
  { label: "Fraîcheur des données temps réel",  value: "< 5 minutes",  target: "300 secondes max",   status: "healthy" },
  { label: "Disponibilité du pipeline",          value: "99%",           target: "7h maxi d'arrêt/mois", status: "healthy" },
  { label: "Fréquence d'ingestion",              value: "Toutes les 2 min", target: "720 cycles/jour",  status: "healthy" },
  { label: "Rétention données brutes (trips)",   value: "30 jours",      target: "Bronze DuckDB",      status: "healthy" },
  { label: "Rétention logs d'ingestion",         value: "90 jours",      target: "Bronze fetch_log",   status: "healthy" },
  { label: "Temps de réponse API",               value: "< 200ms",       target: "Endpoints Gold",     status: "healthy" },
];

const LINEAGE_STEPS = [
  {
    step: 1,
    from: "SNCF Voyageurs",
    to:   "GTFS-RT Protobuf",
    desc: "La SNCF publie un fichier binaire mis à jour toutes les 2 minutes sur transport.data.gouv.fr",
    color: "#22d3ee",
  },
  {
    step: 2,
    from: "Fetcher Python",
    to:   "bronze.trip_updates",
    desc: "httpx télécharge le Protobuf, le parser le décode en objets Pydantic, DuckDB stocke le snapshot brut",
    color: "#8b5cf6",
  },
  {
    step: 3,
    from: "bronze.trip_updates",
    to:   "silver.delay_events",
    desc: "Les retards > 5 min sont isolés dans des événements Silver pour les agents LangChain (V2)",
    color: "#a78bfa",
  },
  {
    step: 4,
    from: "silver.data_quality_checks",
    to:   "gold.kpi_quality_score_daily",
    desc: "4 contrôles qualité automatiques par cycle (fraîcheur, dispo, complétude, validité) agrégés en score journalier",
    color: "#10b981",
  },
  {
    step: 5,
    from: "bronze → silver → gold",
    to:   "FastAPI / Dashboard",
    desc: "Les KPIs Gold sont exposés via FastAPI et affichés dans le dashboard Next.js en temps réel",
    color: "#fbbf24",
  },
];

// ── Composants ─────────────────────────────────────────────────────────────

function SectionTitle({ children, sub }: { children: React.ReactNode; sub?: string }) {
  return (
    <div className="mb-6">
      <h2 className="text-2xl font-700 text-white tracking-tight">{children}</h2>
      {sub && <p className="text-sm text-white/35 font-300 mt-1">{sub}</p>}
    </div>
  );
}

function ScoreBar({ value, color }: { value: number; color: string }) {
  return (
    <div className="flex items-center gap-3">
      <div className="flex-1 progress-track">
        <div className="progress-fill"
             style={{ width: `${Math.round(value * 100)}%`, background: color, boxShadow: `0 0 5px ${color}` }} />
      </div>
      <span className="text-xs font-600 w-10 text-right" style={{ color }}>
        {Math.round(value * 100)}%
      </span>
    </div>
  );
}

function CatalogCard({ source }: { source: CatalogEntry }) {
  const [open, setOpen] = useState(false);
  const st = STATUS_CONFIG[source.current_status as keyof typeof STATUS_CONFIG] ?? STATUS_CONFIG.unknown;
  const score = source.last_quality_score ?? 0;
  const scoreColor = score >= 0.9 ? "#10b981" : score >= 0.7 ? "#fbbf24" : "#f472b6";

  return (
    <div className={`card border ${st.border} overflow-visible`}>
      <div className="p-5">
        {/* En-tête */}
        <div className="flex items-start justify-between gap-3 mb-4">
          <div className="flex-1 min-w-0">
            <h3 className="font-600 text-base text-white/80 mb-1">{source.source_name}</h3>
            <p className="text-xs text-white/30 font-300">
              {FEED_TYPE_FR[source.feed_type ?? ""] ?? source.feed_type}
            </p>
          </div>
          <div className={`flex-shrink-0 flex items-center gap-1.5 px-3 py-1.5 rounded-lg ${st.bg} border ${st.border}`}>
            <span className="text-xs font-600" style={{ color: st.color }}>{st.icon} {st.label}</span>
          </div>
        </div>

        {/* Score qualité */}
        <div className="mb-4">
          <div className="flex items-center justify-between mb-2">
            <span className="text-[10px] tracking-widest text-white/25 uppercase font-500">Score qualité</span>
            <span className="text-xs font-600" style={{ color: scoreColor }}>
              {Math.round(score * 100)}%
            </span>
          </div>
          <ScoreBar value={score} color={scoreColor} />
        </div>

        {/* Méta */}
        <div className="grid grid-cols-2 gap-3 text-xs mb-4">
          <div>
            <span className="text-white/20 font-300 block mb-0.5">Data Owner</span>
            <span className="text-white/50 font-400">{source.owner ?? "—"}</span>
          </div>
          <div>
            <span className="text-white/20 font-300 block mb-0.5">SLA fraîcheur</span>
            <span className="text-white/50 font-400">
              {source.sla_freshness_s ? `< ${source.sla_freshness_s}s` : "—"}
            </span>
          </div>
          <div>
            <span className="text-white/20 font-300 block mb-0.5">Disponibilité cible</span>
            <span className="text-white/50 font-400">
              {source.sla_availability ? `${Math.round(source.sla_availability * 100)}%` : "—"}
            </span>
          </div>
          <div>
            <span className="text-white/20 font-300 block mb-0.5">Dernier fetch</span>
            <span className="text-white/50 font-400">
              {source.last_fetch_at?.slice(11, 19) ?? "—"}
            </span>
          </div>
        </div>

        {/* Description */}
        {source.description && (
          <p className="text-xs text-white/30 font-300 leading-relaxed mb-4 border-t border-white/[0.04] pt-3">
            {source.description}
          </p>
        )}

        {/* URL */}
        {source.source_url && (
          <div className="flex items-center gap-2">
            <span className="text-[9px] text-white/15 font-300 truncate flex-1 font-mono">
              {source.source_url}
            </span>
            <button
              onClick={() => setOpen(!open)}
              className="flex-shrink-0 text-[9px] text-violet-400/60 hover:text-violet-400 font-500 tracking-widest uppercase transition-colors"
            >
              {open ? "Masquer ▲" : "Data Contract ▼"}
            </button>
          </div>
        )}
      </div>

      {/* Data Contract expandable */}
      {open && (
        <div className="border-t border-white/[0.06] bg-white/[0.02] p-5">
          <p className="text-[10px] tracking-widest text-violet-400/60 uppercase font-500 mb-3">
            Data Contract — {source.source_id}
          </p>
          <pre className="text-[10px] text-white/40 font-mono leading-relaxed whitespace-pre-wrap">
{`source_id: "${source.source_id}"
source_name: "${source.source_name}"
owner: "${source.owner}"
steward: "Michel DUPONT (heykelh)"
license: "ODbL"
feed_type: "${source.feed_type}"

quality_rules:
  freshness_max_seconds: ${source.sla_freshness_s ?? 120}
  min_entity_count: 100
  max_error_rate: 0.05
  availability_target: ${source.sla_availability ?? 0.99}

sla:
  freshness_target: "${source.sla_freshness_s ? `< ${source.sla_freshness_s}s` : "< 120s"}"
  availability_target: "${source.sla_availability ? `${Math.round(source.sla_availability * 100)}%` : "99%"}"
  support: "data_office_secretariat_general_sa_voyageurs@sncf.fr"

current_status: "${source.current_status ?? "unknown"}"
last_quality_score: ${source.last_quality_score?.toFixed(4) ?? "null"}
last_fetch_at: "${source.last_fetch_at ?? "null"}"`}
          </pre>
        </div>
      )}
    </div>
  );
}

function QualityCheckCard({ sourceName, data }: { sourceName: string; data: QualitySource }) {
  const overallColor = data.overall_score >= 0.9 ? "#10b981"
    : data.overall_score >= 0.7 ? "#fbbf24" : "#f472b6";

  return (
    <div className="card p-5">
      <div className="flex items-center justify-between mb-5">
        <h3 className="font-600 text-sm text-white/70">{sourceName}</h3>
        <div className="flex items-center gap-2">
          <span className="text-xs text-white/25 font-300">Score global</span>
          <span className="font-700 text-lg" style={{ color: overallColor, textShadow: `0 0 8px ${overallColor}` }}>
            {Math.round(data.overall_score * 100)}%
          </span>
        </div>
      </div>
      <div className="space-y-4">
        {Object.entries(data.checks).map(([checkName, check]) => {
          const cfg = CHECK_FR[checkName];
          const col = check.avg_score >= 0.9 ? "#10b981"
            : check.avg_score >= 0.7 ? "#fbbf24" : "#f472b6";
          return (
            <div key={checkName}>
              <div className="flex items-center justify-between mb-1.5">
                <div className="flex items-center gap-2">
                  <span className="text-sm">{cfg?.icon ?? "🔍"}</span>
                  <span className="text-xs font-500 text-white/60">
                    {cfg?.label ?? checkName}
                  </span>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-[9px] text-white/20 font-300">
                    {check.passed}/{check.total} ok
                  </span>
                  <span className="text-xs font-600" style={{ color: col }}>
                    {Math.round(check.pass_rate)}%
                  </span>
                </div>
              </div>
              <ScoreBar value={check.avg_score} color={col} />
              {cfg?.explain && (
                <p className="text-[10px] text-white/20 font-300 mt-1 leading-relaxed">
                  {cfg.explain}
                </p>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Page ───────────────────────────────────────────────────────────────────

export default function GovernancePage() {
  const [catalog, setCatalog]   = useState<CatalogEntry[]>([]);
  const [quality, setQuality]   = useState<Record<string, QualitySource>>({});
  const [summary, setSummary]   = useState<Record<string, { total_records: number; earliest: string; latest: string; avg_punctuality: number }>>({});
  const [loading, setLoading]   = useState(true);
  const [activeTab, setActiveTab] = useState<"catalog" | "quality" | "lineage" | "roles">("catalog");

  const load = useCallback(async () => {
    try {
      const [catData, qualData, sumData] = await Promise.all([
        api.governanceCatalog(),
        api.kpiQuality(24),
        api.regularitySummary(),
      ]);
      setCatalog(catData.sources);
      setQuality(qualData.sources);
      setSummary(sumData.summary as Record<string, { total_records: number; earliest: string; latest: string; avg_punctuality: number }>);
      setLoading(false);
    } catch (err) {
      console.error(err);
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  if (loading) return (
    <div className="min-h-[80vh] flex items-center justify-center">
      <div className="text-center space-y-4">
        <div className="w-8 h-8 border-2 border-violet-500/30 border-t-violet-400 rounded-full animate-spin mx-auto" />
        <p className="text-lg font-600 text-white/60">Chargement de la gouvernance...</p>
      </div>
    </div>
  );

  const tabs = [
    { key: "catalog",  label: "Data Catalog",     icon: "📚" },
    { key: "quality",  label: "Qualité des données", icon: "🔍" },
    { key: "lineage",  label: "Lignage",           icon: "🔗" },
    { key: "roles",    label: "Rôles & SLA",       icon: "👥" },
  ] as const;

  return (
    <div className="bg-grid min-h-screen">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-10 space-y-8">

        {/* En-tête */}
        <div>
          <h1 className="text-4xl sm:text-5xl font-700 tracking-tight text-white leading-none mb-3">
            Gouvernance des données
          </h1>
          <p className="text-base text-white/35 font-300 max-w-2xl">
            Transparence totale sur les sources de données, leur qualité, leur traçabilité
            et les responsabilités associées. Conforme au standard DAMA-DMBOK.
          </p>
        </div>

        {/* Bandeau DAMA */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {[
            { label: "Sources documentées",    value: catalog.length,                        color: "#8b5cf6" },
            { label: "Enregistrements historiques", value: Object.values(summary).reduce((a, s) => a + (s.total_records ?? 0), 0).toLocaleString("fr"), color: "#10b981" },
            { label: "Contrôles qualité / 24h", value: Object.values(quality).reduce((a, s) => a + Object.values(s.checks).reduce((b, c) => b + c.total, 0), 0), color: "#22d3ee" },
            { label: "Score qualité moyen",     value: (() => { const scores = Object.values(quality).map(s => s.overall_score).filter(Boolean); return scores.length ? `${Math.round(scores.reduce((a,b)=>a+b,0)/scores.length*100)}%` : "—"; })(), color: "#fbbf24" },
          ].map(({ label, value, color }) => (
            <div key={label} className="card p-4">
              <span className="text-[10px] tracking-widest text-white/25 uppercase font-500 block mb-2">{label}</span>
              <span className="font-700 text-3xl leading-none" style={{ color, textShadow: `0 0 10px ${color}` }}>
                {value}
              </span>
            </div>
          ))}
        </div>

        {/* Tabs */}
        <div className="flex flex-wrap gap-2 border-b border-white/[0.06] pb-0">
          {tabs.map(t => (
            <button
              key={t.key}
              onClick={() => setActiveTab(t.key)}
              className={`flex items-center gap-2 px-4 py-3 text-sm font-500 transition-all border-b-2 -mb-px ${
                activeTab === t.key
                  ? "border-violet-500 text-violet-300"
                  : "border-transparent text-white/35 hover:text-white/60"
              }`}
            >
              <span>{t.icon}</span>
              <span>{t.label}</span>
            </button>
          ))}
        </div>

        {/* ── Tab : Data Catalog ── */}
        {activeTab === "catalog" && (
          <div className="space-y-6">
            <SectionTitle
              sub="Chaque source de données est documentée avec ses propriétaires, ses règles de qualité et son contrat de données."
            >
              📚 Data Catalog
            </SectionTitle>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              {catalog.map(source => (
                <CatalogCard key={source.source_id} source={source} />
              ))}
            </div>

            {/* Données historiques */}
            <div className="card p-6 mt-6">
              <h3 className="font-600 text-base text-white/70 mb-1">
                📅 Données historiques de régularité
              </h3>
              <p className="text-sm text-white/30 font-300 mb-5">
                Séries temporelles de ponctualité mensuelle depuis 2013 — chargées depuis l&apos;Open Data SNCF.
              </p>
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-white/[0.06]">
                      {["Type de train", "Enregistrements", "Depuis", "Jusqu'à", "Ponctualité moyenne"].map(h => (
                        <th key={h} className="text-left py-2 pr-6 text-[9px] tracking-widest text-white/20 font-500 uppercase">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(summary).map(([type, s]) => {
                      const avg = s.avg_punctuality;
                      const col = !avg ? "#6b7280" : avg >= 90 ? "#10b981" : avg >= 80 ? "#fbbf24" : "#f472b6";
                      const typeLabel: Record<string, string> = {
                        tgv: "🚄 TGV", tgv_axe: "🚄 TGV par axe",
                        ter: "🚃 TER", transilien: "🚇 Transilien", intercites: "🚂 Intercités",
                      };
                      return (
                        <tr key={type} className="border-b border-white/[0.03] hover:bg-white/[0.02] transition-colors">
                          <td className="py-3 pr-6 font-500 text-white/60">{typeLabel[type] ?? type}</td>
                          <td className="py-3 pr-6 text-white/40">{s.total_records?.toLocaleString("fr")}</td>
                          <td className="py-3 pr-6 text-white/40">{s.earliest}</td>
                          <td className="py-3 pr-6 text-white/40">{s.latest}</td>
                          <td className="py-3">
                            <span className="font-600" style={{ color: col }}>
                              {avg ? `${avg.toFixed(1)}%` : "—"}
                            </span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* ── Tab : Qualité ── */}
        {activeTab === "quality" && (
          <div className="space-y-6">
            <SectionTitle
              sub="4 contrôles automatiques exécutés à chaque cycle d'ingestion (toutes les 2 minutes). Résultats des dernières 24 heures."
            >
              🔍 Contrôles de qualité automatiques
            </SectionTitle>

            {/* Explication des contrôles */}
            <div className="bg-violet-500/5 border border-violet-500/15 rounded-xl p-5">
              <p className="text-sm text-white/50 font-300 leading-relaxed">
                <span className="text-violet-300 font-500">Comment fonctionne le Data Quality Framework ? </span>
                À chaque fois que le pipeline récupère les données SNCF (toutes les 2 minutes),
                4 contrôles automatiques sont exécutés et leur résultat est enregistré dans la base de données.
                Ces contrôles permettent de détecter immédiatement si une source se dégrade ou disparaît.
              </p>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              {Object.entries(quality).map(([sourceId, data]) => {
                const sourceNames: Record<string, string> = {
                  trip_updates:   "GTFS-RT Trip Updates",
                  service_alerts: "GTFS-RT Service Alerts",
                };
                return (
                  <QualityCheckCard
                    key={sourceId}
                    sourceName={sourceNames[sourceId] ?? sourceId}
                    data={data}
                  />
                );
              })}
            </div>

            {Object.keys(quality).length === 0 && (
              <div className="card p-12 text-center">
                <p className="text-white/30 font-300">
                  Les contrôles qualité s&apos;affichent après le premier cycle d&apos;ingestion.
                </p>
              </div>
            )}
          </div>
        )}

        {/* ── Tab : Lignage ── */}
        {activeTab === "lineage" && (
          <div className="space-y-6">
            <SectionTitle
              sub="Traçabilité complète du flux de données, de la source SNCF jusqu'à l'affichage dans le dashboard."
            >
              🔗 Data Lineage — Traçabilité des données
            </SectionTitle>

            <div className="bg-violet-500/5 border border-violet-500/15 rounded-xl p-5 mb-6">
              <p className="text-sm text-white/50 font-300 leading-relaxed">
                <span className="text-violet-300 font-500">Qu&apos;est-ce que le Data Lineage ? </span>
                C&apos;est la carte qui montre d&apos;où vient chaque donnée, comment elle est transformée
                et où elle arrive. Indispensable pour comprendre, auditer et corriger les données
                en cas de problème.
              </p>
            </div>

            {/* Architecture medallion */}
            <div className="card p-6 mb-4">
              <h3 className="font-600 text-sm text-white/60 mb-5 uppercase tracking-widest text-xs">
                Architecture Medallion — Bronze / Silver / Gold
              </h3>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                {[
                  {
                    name: "Bronze", color: "#cd7f32",
                    icon: "🥉",
                    desc: "Données brutes telles qu'elles arrivent de la SNCF. Rien n'est modifié, tout est gardé.",
                    tables: ["fetch_log", "trip_updates", "stop_time_updates", "service_alerts", "regularity_raw", "stops"],
                  },
                  {
                    name: "Silver", color: "#C0C0C0",
                    icon: "🥈",
                    desc: "Données nettoyées, dédupliquées et enrichies. Prêtes pour l'analyse.",
                    tables: ["trips_deduped", "delay_events", "data_quality_checks"],
                  },
                  {
                    name: "Gold", color: "#FFD700",
                    icon: "🥇",
                    desc: "KPIs agrégés, directement affichés dans le dashboard. Optimisés pour la performance.",
                    tables: ["kpi_punctuality_hourly", "kpi_quality_score_daily", "kpi_alerts_summary", "data_catalog_metadata"],
                  },
                ].map(layer => (
                  <div key={layer.name} className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-4">
                    <div className="flex items-center gap-2 mb-3">
                      <span className="text-xl">{layer.icon}</span>
                      <span className="font-700 text-base" style={{ color: layer.color }}>
                        {layer.name}
                      </span>
                    </div>
                    <p className="text-xs text-white/35 font-300 mb-3 leading-relaxed">{layer.desc}</p>
                    <div className="space-y-1">
                      {layer.tables.map(t => (
                        <div key={t} className="flex items-center gap-2">
                          <span className="w-1 h-1 rounded-full flex-shrink-0" style={{ background: layer.color }} />
                          <span className="text-[10px] font-mono text-white/30">{t}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Étapes du lineage */}
            <div className="card p-6">
              <h3 className="font-600 text-sm text-white/60 mb-5 uppercase tracking-widest text-xs">
                Parcours d&apos;une donnée — de la SNCF au dashboard
              </h3>
              <div className="space-y-0">
                {LINEAGE_STEPS.map((step, i) => (
                  <div key={step.step} className="flex gap-4">
                    {/* Ligne verticale */}
                    <div className="flex flex-col items-center">
                      <div className="w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 border-2"
                           style={{ borderColor: step.color, background: `${step.color}15` }}>
                        <span className="text-xs font-700" style={{ color: step.color }}>{step.step}</span>
                      </div>
                      {i < LINEAGE_STEPS.length - 1 && (
                        <div className="w-px flex-1 my-1" style={{ background: `${step.color}30`, minHeight: "24px" }} />
                      )}
                    </div>
                    {/* Contenu */}
                    <div className="pb-6 flex-1 min-w-0">
                      <div className="flex flex-wrap items-center gap-2 mb-1">
                        <span className="font-600 text-sm" style={{ color: step.color }}>{step.from}</span>
                        <span className="text-white/20">→</span>
                        <span className="font-600 text-sm text-white/60">{step.to}</span>
                      </div>
                      <p className="text-xs text-white/30 font-300 leading-relaxed">{step.desc}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* ── Tab : Rôles & SLA ── */}
        {activeTab === "roles" && (
          <div className="space-y-6">
            <SectionTitle
              sub="Définition des responsabilités et des engagements de service — conforme DAMA-DMBOK."
            >
              👥 Rôles & Responsabilités
            </SectionTitle>

            {/* RACI */}
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              {ROLE_FR.map(r => (
                <div key={r.role} className="card p-5">
                  <div className="flex items-center gap-3 mb-3">
                    <span className="text-2xl">{r.icon}</span>
                    <div>
                      <p className="font-600 text-sm text-violet-300">{r.role}</p>
                      <p className="text-xs text-white/50 font-400">{r.who}</p>
                    </div>
                  </div>
                  <p className="text-xs text-white/30 font-300 leading-relaxed">{r.what}</p>
                </div>
              ))}
            </div>

            {/* SLA */}
            <div>
              <h3 className="text-xs font-500 tracking-widest text-white/25 uppercase mb-4">
                Engagements de service (SLA)
              </h3>
              <div className="card overflow-hidden">
                <div className="hidden sm:grid grid-cols-[1fr_120px_180px_100px] gap-4 px-6 py-3 border-b border-white/[0.04] bg-white/[0.02]">
                  {["Indicateur", "Valeur actuelle", "Objectif", "Statut"].map(h => (
                    <span key={h} className="text-[9px] tracking-widest text-white/20 font-500 uppercase">{h}</span>
                  ))}
                </div>
                {SLA_ITEMS.map((item, i) => {
                  const st = STATUS_CONFIG[item.status as keyof typeof STATUS_CONFIG];
                  return (
                    <div key={i}
                      className="grid grid-cols-1 sm:grid-cols-[1fr_120px_180px_100px] gap-2 sm:gap-4 px-6 py-4 border-b border-white/[0.03] hover:bg-white/[0.015] transition-colors">
                      <span className="text-sm text-white/60 font-400">{item.label}</span>
                      <span className="text-sm font-600 text-violet-300">{item.value}</span>
                      <span className="text-xs text-white/30 font-300">{item.target}</span>
                      <div className={`flex items-center gap-1.5 px-2 py-1 rounded-lg w-fit ${st.bg} border ${st.border}`}>
                        <span className="text-[10px] font-600" style={{ color: st.color }}>
                          {st.icon} {st.label}
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Standards */}
            <div className="card p-6">
              <h3 className="font-600 text-sm text-white/60 mb-4">Standards et références</h3>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-xs text-white/35 font-300">
                {[
                  { label: "Framework gouvernance", value: "DAMA-DMBOK v2" },
                  { label: "Format flux temps réel", value: "GTFS-RT (Google Transit)" },
                  { label: "Format horaires statiques", value: "GTFS + NeTEx (norme européenne)" },
                  { label: "Licence données",          value: "Open Database License (ODbL)" },
                  { label: "Architecture stockage",    value: "Medallion (Bronze/Silver/Gold)" },
                  { label: "Outil transformations",    value: "dbt Core (Data Build Tool)" },
                ].map(({ label, value }) => (
                  <div key={label} className="flex items-start gap-3">
                    <span className="w-1.5 h-1.5 rounded-full bg-violet-500/50 flex-shrink-0 mt-1.5" />
                    <div>
                      <span className="text-white/25 block">{label}</span>
                      <span className="text-white/55 font-500">{value}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

      </div>
    </div>
  );
}