"""
Storage · DuckDB Database
==========================
Initialisation, gestion du schéma et connexion DuckDB.

Architecture en 3 couches (alignée avec dbt) :
  - bronze : données brutes as-is depuis les flux GTFS-RT
  - silver : données nettoyées, enrichies, dédupliquées
  - gold   : KPIs agrégés, métriques de qualité (pour le dashboard)

Utilisé par :
  - storage/writer.py          → écriture après chaque fetch
  - api/routers/               → lecture pour FastAPI (semaine 4)
  - dbt/                       → transformations Silver/Gold (semaine 4)
  - agents/anomaly_detector.py → LangChain lit Gold (V2)

Lancer l'init seule :
    python -m storage.database
"""

import os
from contextlib import contextmanager
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DUCKDB_PATH: str = os.getenv("DUCKDB_PATH", "./data/observatory.duckdb")

# S'assure que le dossier data/ existe
Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# DDL — Bronze : données brutes
# ---------------------------------------------------------------------------

_DDL_BRONZE = """

-- -----------------------------------------------------------------------
-- bronze.fetch_log
-- Traçabilité de chaque cycle d'ingestion.
-- C'est la table de gouvernance centrale : on sait exactement quand,
-- depuis quelle URL, avec quel résultat chaque fetch a eu lieu.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.fetch_log (
    id                  VARCHAR     PRIMARY KEY,   -- UUID généré côté writer
    fetched_at          TIMESTAMP   NOT NULL,
    source_url          VARCHAR     NOT NULL,
    feed_type           VARCHAR     NOT NULL,      -- 'trip_updates' | 'service_alerts'
    http_status         INTEGER     NOT NULL,
    entity_count        INTEGER     DEFAULT 0,
    error_count         INTEGER     DEFAULT 0,
    skipped_count       INTEGER     DEFAULT 0,
    parse_duration_ms   DOUBLE,
    feed_timestamp      TIMESTAMP,                 -- timestamp interne du flux GTFS-RT
    data_freshness_s    DOUBLE,                    -- âge de la donnée en secondes
    quality_score       DOUBLE,                    -- score 0.0→1.0 calculé par le parser
    created_at          TIMESTAMP   DEFAULT NOW()
);

-- -----------------------------------------------------------------------
-- bronze.trip_updates
-- Snapshot brut de chaque TripUpdate reçu.
-- Une ligne = un train = un cycle de fetch.
-- On garde tout, même les doublons inter-cycles (Silver déduplique).
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.trip_updates (
    id                  VARCHAR     PRIMARY KEY,   -- fetch_id + entity_id
    fetch_log_id        VARCHAR     NOT NULL,      -- FK → bronze.fetch_log
    fetched_at          TIMESTAMP   NOT NULL,
    entity_id           VARCHAR     NOT NULL,
    trip_id             VARCHAR,
    route_id            VARCHAR,
    direction_id        INTEGER,
    start_time          VARCHAR,                   -- HH:MM:SS
    start_date          VARCHAR,                   -- YYYYMMDD
    trip_schedule_rel   VARCHAR,                   -- SCHEDULED | SKIPPED | UNSCHEDULED
    trip_timestamp      TIMESTAMP,
    stop_count          INTEGER     DEFAULT 0,     -- nb d'arrêts dans ce trip
    max_delay_seconds   INTEGER,                   -- retard max sur tous les arrêts
    max_delay_minutes   DOUBLE,
    has_cancellation    BOOLEAN     DEFAULT FALSE,
    affected_stops      INTEGER     DEFAULT 0,     -- nb arrêts avec retard > 60s
    raw_json            JSON,                      -- dump JSON complet pour debug / LangChain
    created_at          TIMESTAMP   DEFAULT NOW()
);

-- -----------------------------------------------------------------------
-- bronze.stop_time_updates
-- Détail arrêt par arrêt pour chaque trip.
-- Permet de calculer les retards station par station (Silver + Gold).
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.stop_time_updates (
    id                  VARCHAR     PRIMARY KEY,   -- trip_update_id + stop_sequence
    trip_update_id      VARCHAR     NOT NULL,      -- FK → bronze.trip_updates
    fetch_log_id        VARCHAR     NOT NULL,
    fetched_at          TIMESTAMP   NOT NULL,
    stop_sequence       INTEGER,
    stop_id             VARCHAR,
    arrival_delay_s     INTEGER,
    arrival_time        TIMESTAMP,
    departure_delay_s   INTEGER,
    departure_time      TIMESTAMP,
    schedule_rel        VARCHAR,
    delay_seconds       INTEGER,                   -- retard effectif (départ prioritaire)
    delay_minutes       DOUBLE,
    is_delayed          BOOLEAN     DEFAULT FALSE,
    is_cancelled        BOOLEAN     DEFAULT FALSE,
    created_at          TIMESTAMP   DEFAULT NOW()
);

-- -----------------------------------------------------------------------
-- bronze.service_alerts
-- Alertes de perturbation brutes.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.service_alerts (
    id                  VARCHAR     PRIMARY KEY,
    fetch_log_id        VARCHAR     NOT NULL,
    fetched_at          TIMESTAMP   NOT NULL,
    entity_id           VARCHAR     NOT NULL,
    cause               VARCHAR,
    effect              VARCHAR,
    header_text         VARCHAR,
    description_text    VARCHAR,
    active_period_start TIMESTAMP,
    active_period_end   TIMESTAMP,
    is_active_now       BOOLEAN,
    is_cancellation     BOOLEAN     DEFAULT FALSE,
    raw_json            JSON,
    created_at          TIMESTAMP   DEFAULT NOW()
);

"""

# ---------------------------------------------------------------------------
# DDL — Silver : données nettoyées et enrichies
# ---------------------------------------------------------------------------

_DDL_SILVER = """

-- -----------------------------------------------------------------------
-- silver.trips_deduped
-- Vue matérialisée : un seul enregistrement par (trip_id, start_date).
-- On garde le dernier fetch pour chaque trip (fenêtre glissante).
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.trips_deduped (
    trip_id             VARCHAR     NOT NULL,
    start_date          VARCHAR,
    route_id            VARCHAR,
    direction_id        INTEGER,
    start_time          VARCHAR,
    last_seen_at        TIMESTAMP,                 -- dernier fetch où ce trip était présent
    first_seen_at       TIMESTAMP,
    max_delay_seconds   INTEGER,
    max_delay_minutes   DOUBLE,
    has_cancellation    BOOLEAN,
    affected_stops      INTEGER,
    fetch_count         INTEGER     DEFAULT 1,     -- nb de fois vu dans les fetches
    PRIMARY KEY (trip_id, start_date)
);

-- -----------------------------------------------------------------------
-- silver.delay_events
-- Événements de retard significatif (> 5 min) — pour les alertes LangChain
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.delay_events (
    id                  VARCHAR     PRIMARY KEY,
    trip_id             VARCHAR,
    route_id            VARCHAR,
    start_date          VARCHAR,
    detected_at         TIMESTAMP,
    delay_minutes       DOUBLE,
    stop_id             VARCHAR,
    severity            VARCHAR,                   -- 'minor'|'moderate'|'severe'|'critical'
    resolved_at         TIMESTAMP,
    is_resolved         BOOLEAN     DEFAULT FALSE
);

-- -----------------------------------------------------------------------
-- silver.data_quality_checks
-- Résultats des contrôles qualité sur chaque fetch.
-- Alimente le Data Quality Dashboard.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.data_quality_checks (
    id                  VARCHAR     PRIMARY KEY,
    fetch_log_id        VARCHAR     NOT NULL,
    checked_at          TIMESTAMP   NOT NULL,
    check_name          VARCHAR     NOT NULL,      -- 'freshness'|'completeness'|'validity'
    source              VARCHAR     NOT NULL,      -- 'trip_updates'|'service_alerts'
    passed              BOOLEAN     NOT NULL,
    score               DOUBLE,                    -- 0.0 → 1.0
    expected_value      VARCHAR,
    actual_value        VARCHAR,
    details             VARCHAR
);

"""

# ---------------------------------------------------------------------------
# DDL — Gold : agrégats pour le dashboard
# ---------------------------------------------------------------------------

_DDL_GOLD = """

-- -----------------------------------------------------------------------
-- gold.kpi_punctuality_hourly
-- Taux de ponctualité agrégé par heure — affiché dans le dashboard.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.kpi_punctuality_hourly (
    hour_bucket         TIMESTAMP   NOT NULL,      -- tronqué à l'heure
    total_trips         INTEGER     DEFAULT 0,
    delayed_trips       INTEGER     DEFAULT 0,
    cancelled_trips     INTEGER     DEFAULT 0,
    on_time_trips       INTEGER     DEFAULT 0,
    punctuality_rate    DOUBLE,                    -- on_time / total
    avg_delay_minutes   DOUBLE,
    max_delay_minutes   DOUBLE,
    quality_score_avg   DOUBLE,                    -- moyenne des quality_scores des fetches
    PRIMARY KEY (hour_bucket)
);

-- -----------------------------------------------------------------------
-- gold.kpi_quality_score_daily
-- Score de qualité des données par jour et par source.
-- Permet de monitorer la fiabilité de l'open data SNCF dans le temps.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.kpi_quality_score_daily (
    day_bucket          DATE        NOT NULL,
    source              VARCHAR     NOT NULL,
    fetch_count         INTEGER,
    avg_quality_score   DOUBLE,
    min_quality_score   DOUBLE,
    avg_freshness_s     DOUBLE,
    error_rate          DOUBLE,
    PRIMARY KEY (day_bucket, source)
);

-- -----------------------------------------------------------------------
-- gold.kpi_alerts_summary
-- Résumé des alertes actives — pour la carte et le compteur live.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.kpi_alerts_summary (
    computed_at         TIMESTAMP   PRIMARY KEY,
    total_alerts        INTEGER     DEFAULT 0,
    active_alerts       INTEGER     DEFAULT 0,
    cancellations       INTEGER     DEFAULT 0,
    delays_significant  INTEGER     DEFAULT 0,
    top_cause           VARCHAR,
    top_effect          VARCHAR
);

-- -----------------------------------------------------------------------
-- gold.data_catalog_metadata
-- Catalogue de données public — affiché dans la page Gouvernance.
-- Chaque source y est documentée avec ses SLA et son état courant.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.data_catalog_metadata (
    source_id           VARCHAR     PRIMARY KEY,
    source_name         VARCHAR     NOT NULL,
    source_url          VARCHAR,
    feed_type           VARCHAR,
    owner               VARCHAR,                   -- Data Owner (gouvernance)
    steward             VARCHAR,                   -- Data Steward
    sla_freshness_s     INTEGER,                   -- SLA fraîcheur attendu en secondes
    sla_availability    DOUBLE,                    -- SLA dispo attendu (ex: 0.99)
    last_fetch_at       TIMESTAMP,
    last_quality_score  DOUBLE,
    current_status      VARCHAR,                   -- 'healthy'|'degraded'|'down'
    description         VARCHAR,
    updated_at          TIMESTAMP   DEFAULT NOW()
);

"""

# ---------------------------------------------------------------------------
# DDL — Index pour les performances de requête
# ---------------------------------------------------------------------------

_DDL_INDEXES = """

CREATE INDEX IF NOT EXISTS idx_trip_updates_fetched_at
    ON bronze.trip_updates (fetched_at);

CREATE INDEX IF NOT EXISTS idx_trip_updates_trip_id
    ON bronze.trip_updates (trip_id);

CREATE INDEX IF NOT EXISTS idx_trip_updates_route_id
    ON bronze.trip_updates (route_id);

CREATE INDEX IF NOT EXISTS idx_stop_updates_trip_id
    ON bronze.stop_time_updates (trip_update_id);

CREATE INDEX IF NOT EXISTS idx_stop_updates_stop_id
    ON bronze.stop_time_updates (stop_id);

CREATE INDEX IF NOT EXISTS idx_alerts_fetched_at
    ON bronze.service_alerts (fetched_at);

CREATE INDEX IF NOT EXISTS idx_fetch_log_fetched_at
    ON bronze.fetch_log (fetched_at);

CREATE INDEX IF NOT EXISTS idx_quality_checks_source
    ON silver.data_quality_checks (source, checked_at);

"""

# ---------------------------------------------------------------------------
# Données initiales — Data Catalog (gouvernance)
# ---------------------------------------------------------------------------

_DDL_SEED_CATALOG = """

INSERT OR REPLACE INTO gold.data_catalog_metadata VALUES
(
    'gtfs-rt-trip-updates',
    'GTFS-RT Trip Updates SNCF',
    'https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates',
    'trip_updates',
    'SNCF Voyageurs / Open Data SNCF',
    'SNCF Data Observatory (heykelh)',
    120,
    0.99,
    NULL, NULL, 'unknown',
    'Flux temps réel des retards et suppressions TGV/IC/TER — mis à jour toutes les 2 min. Format Protobuf GTFS-RT. Périmètre : trains circulant dans les 60 prochaines minutes.',
    NOW()
),
(
    'gtfs-rt-service-alerts',
    'GTFS-RT Service Alerts SNCF',
    'https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-service-alerts',
    'service_alerts',
    'SNCF Voyageurs / Open Data SNCF',
    'SNCF Data Observatory (heykelh)',
    120,
    0.99,
    NULL, NULL, 'unknown',
    'Flux temps réel des alertes de perturbation TGV/IC/TER/OUIGO. Incidents, suppressions, travaux. Format Protobuf GTFS-RT.',
    NOW()
),
(
    'sncf-regularity-tgv',
    'Régularité mensuelle TGV',
    'https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/reglarite-mensuelle-tgv-nationale/exports/json',
    'regularity_historical',
    'SNCF Voyageurs / Open Data SNCF',
    'SNCF Data Observatory (heykelh)',
    2592000,
    0.95,
    NULL, NULL, 'unknown',
    'Taux de ponctualité mensuel TGV depuis 2013. Données AQST. Mis à jour mensuellement.',
    NOW()
),
(
    'sncf-regularity-ter',
    'Régularité mensuelle TER',
    'https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/regularite-mensuelle-ter/exports/json',
    'regularity_historical',
    'SNCF Voyageurs / Open Data SNCF',
    'SNCF Data Observatory (heykelh)',
    2592000,
    0.95,
    NULL, NULL, 'unknown',
    'Taux de ponctualité mensuel TER depuis 2013. Données AQST. Mis à jour mensuellement.',
    NOW()
),
(
    'sncf-regularity-transilien',
    'Régularité mensuelle Transilien',
    'https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/ponctualite-mensuelle-transilien/exports/json',
    'regularity_historical',
    'SNCF Voyageurs / Open Data SNCF',
    'SNCF Data Observatory (heykelh)',
    2592000,
    0.95,
    NULL, NULL, 'unknown',
    'Taux de ponctualité mensuel Transilien depuis 2013. Mis à jour mensuellement.',
    NOW()
);

"""


# ---------------------------------------------------------------------------
# Connexion DuckDB
# ---------------------------------------------------------------------------

def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Retourne une connexion DuckDB ouverte.
    À fermer après usage : conn.close()
    Préférer le context manager get_db() pour une gestion automatique.
    """
    return duckdb.connect(DUCKDB_PATH, read_only=read_only)


@contextmanager
def get_db(read_only: bool = False):
    """
    Context manager pour une connexion DuckDB propre.

    Usage :
        with get_db() as conn:
            conn.execute("SELECT ...")

    Gère automatiquement la fermeture et les erreurs.
    """
    conn = duckdb.connect(DUCKDB_PATH, read_only=read_only)
    try:
        yield conn
        conn.commit()
    except Exception as exc:
        logger.error(f"Erreur DuckDB : {exc}")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Initialisation du schéma
# ---------------------------------------------------------------------------

def init_db(force_reset: bool = False) -> None:
    """
    Crée les schémas, tables et index s'ils n'existent pas.
    Idempotent : safe à appeler à chaque démarrage.

    Args:
        force_reset: Si True, supprime et recrée toutes les tables.
                     ATTENTION : perte de données — dev uniquement.
    """
    logger.info(f"Initialisation DuckDB → {DUCKDB_PATH}")

    with get_db() as conn:

        if force_reset:
            logger.warning("FORCE RESET — suppression de tous les schémas")
            for schema in ("gold", "silver", "bronze"):
                conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

        # Création des schémas
        for schema in ("bronze", "silver", "gold"):
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            logger.debug(f"Schéma '{schema}' prêt")

        # Tables
        conn.execute(_DDL_BRONZE)
        logger.debug("Tables Bronze créées")

        conn.execute(_DDL_SILVER)
        logger.debug("Tables Silver créées")

        conn.execute(_DDL_GOLD)
        logger.debug("Tables Gold créées")

        # Index
        conn.execute(_DDL_INDEXES)
        logger.debug("Index créés")

        # Données initiales catalogue
        conn.execute(_DDL_SEED_CATALOG)
        logger.debug("Data Catalog initialisé")

    logger.success(f"DuckDB initialisé avec succès → {DUCKDB_PATH}")


# ---------------------------------------------------------------------------
# Utilitaires de diagnostic
# ---------------------------------------------------------------------------

def get_table_stats() -> dict:
    """
    Retourne le nombre de lignes dans chaque table principale.
    Utilisé par FastAPI /health et le dashboard de gouvernance.
    """
    tables = {
        "bronze.fetch_log":              "fetch_log",
        "bronze.trip_updates":           "trip_updates",
        "bronze.stop_time_updates":      "stop_time_updates",
        "bronze.service_alerts":         "service_alerts",
        "silver.trips_deduped":          "trips_deduped",
        "silver.delay_events":           "delay_events",
        "silver.data_quality_checks":    "quality_checks",
        "gold.kpi_punctuality_hourly":   "kpi_punctuality",
        "gold.kpi_quality_score_daily":  "kpi_quality_daily",
        "gold.kpi_alerts_summary":       "kpi_alerts",
    }
    stats = {}
    with get_db(read_only=True) as conn:
        for full_name, short in tables.items():
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]
                stats[short] = count
            except Exception:
                stats[short] = -1  # Table inaccessible
    return stats


def get_db_size_mb() -> float:
    """Retourne la taille du fichier DuckDB en MB."""
    path = Path(DUCKDB_PATH)
    if path.exists():
        return round(path.stat().st_size / (1024 * 1024), 2)
    return 0.0


def get_latest_fetch_info() -> dict | None:
    """
    Retourne les infos du dernier fetch réussi.
    Utilisé par FastAPI /status et le dashboard.
    """
    with get_db(read_only=True) as conn:
        row = conn.execute("""
            SELECT
                fetched_at,
                feed_type,
                entity_count,
                quality_score,
                data_freshness_s,
                http_status
            FROM bronze.fetch_log
            WHERE http_status = 200
            ORDER BY fetched_at DESC
            LIMIT 1
        """).fetchone()

    if not row:
        return None

    return {
        "fetched_at":      str(row[0]),
        "feed_type":       row[1],
        "entity_count":    row[2],
        "quality_score":   row[3],
        "freshness_s":     row[4],
        "http_status":     row[5],
    }


# ---------------------------------------------------------------------------
# Point d'entrée standalone : python -m storage.database
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from rich.console import Console
    from rich.table import Table

    console = Console()
    console.rule("[bold cyan]SNCF Data Observatory — DuckDB Init[/bold cyan]")

    init_db()

    stats = get_table_stats()
    size  = get_db_size_mb()

    table = Table(title="État de la base de données", header_style="bold cyan")
    table.add_column("Table",  style="dim")
    table.add_column("Lignes", justify="right")

    for name, count in stats.items():
        color = "red" if count == -1 else "green" if count > 0 else "dim"
        table.add_row(name, f"[{color}]{count}[/{color}]")

    console.print(table)
    console.print(f"\n  Fichier DuckDB : [bold]{DUCKDB_PATH}[/bold] ({size} MB)\n")