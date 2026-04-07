"""
Storage · Writer
================
Persiste les FetchResult (sortie du parser) dans DuckDB.
Appelé après chaque cycle d'ingestion dans fetcher.py.

Responsabilités :
  - Écrire dans bronze.fetch_log (traçabilité)
  - Écrire dans bronze.trip_updates + bronze.stop_time_updates
  - Écrire dans bronze.service_alerts
  - Écrire dans silver.data_quality_checks (contrôles qualité automatiques)
  - Mettre à jour gold.data_catalog_metadata (statut live de la source)

Ce fichier est le seul point d'écriture dans DuckDB.
Le parser ne sait pas qu'une DB existe.
FastAPI ne sait pas comment les données sont stockées.
→ Couplage minimal = architecture propre.
"""

import json
import uuid
from datetime import datetime, UTC

from loguru import logger

from ingestion.gtfs_rt.models import FetchResult, ServiceAlert, TripUpdate
from storage.database import get_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_id() -> str:
    """Génère un UUID4 court (sans tirets) pour les PKs."""
    return uuid.uuid4().hex


def _to_json(obj) -> str:
    """Sérialise un modèle Pydantic en JSON string pour la colonne raw_json."""
    try:
        return obj.model_dump_json()
    except Exception:
        return "{}"


def _delay_severity(delay_minutes: float | None) -> str:
    """Classe la sévérité d'un retard pour silver.delay_events."""
    if delay_minutes is None:
        return "none"
    if delay_minutes < 5:
        return "minor"
    if delay_minutes < 15:
        return "moderate"
    if delay_minutes < 30:
        return "severe"
    return "critical"


# ---------------------------------------------------------------------------
# Écriture bronze.fetch_log
# ---------------------------------------------------------------------------

def _write_fetch_log(conn, result: FetchResult, feed_type: str) -> str:
    """
    Insère une ligne dans bronze.fetch_log.
    Retourne le fetch_log_id généré (utilisé comme FK dans les autres tables).
    """
    fetch_id = _new_id()

    conn.execute("""
        INSERT INTO bronze.fetch_log (
            id, fetched_at, source_url, feed_type,
            http_status, entity_count, error_count, skipped_count,
            parse_duration_ms, feed_timestamp, data_freshness_s, quality_score
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        fetch_id,
        result.fetched_at,
        result.source_url,
        feed_type,
        result.http_status,
        result.entity_count,
        result.error_count,
        result.skipped_count,
        result.parse_duration_ms,
        result.feed_timestamp,
        result.data_freshness_seconds,
        result.quality_score,
    ])

    return fetch_id


# ---------------------------------------------------------------------------
# Écriture bronze.trip_updates + bronze.stop_time_updates
# ---------------------------------------------------------------------------

def _write_trip_updates(conn, result: FetchResult, fetch_log_id: str) -> int:
    """
    Insère tous les TripUpdates d'un FetchResult.
    Retourne le nombre de trips écrits.
    """
    written = 0

    for trip in result.trip_updates:
        trip_pk = f"{fetch_log_id}_{trip.entity_id}"

        try:
            conn.execute("""
                INSERT OR IGNORE INTO bronze.trip_updates (
                    id, fetch_log_id, fetched_at, entity_id,
                    trip_id, route_id, direction_id, start_time, start_date,
                    trip_schedule_rel, trip_timestamp,
                    stop_count, max_delay_seconds, max_delay_minutes,
                    has_cancellation, affected_stops, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                trip_pk,
                fetch_log_id,
                result.fetched_at,
                trip.entity_id,
                trip.trip.trip_id,
                trip.trip.route_id,
                trip.trip.direction_id,
                trip.trip.start_time,
                trip.trip.start_date,
                trip.trip.schedule_relationship.value,
                trip.timestamp,
                len(trip.stop_time_update),
                trip.max_delay_seconds,
                trip.max_delay_minutes,
                trip.has_cancellation,
                trip.affected_stops_count,
                _to_json(trip),
            ])

            # Écriture des stop_time_updates associés
            _write_stop_time_updates(conn, trip, trip_pk, fetch_log_id, result.fetched_at)
            written += 1

        except Exception as exc:
            logger.warning(f"Erreur écriture trip {trip.entity_id}: {exc}")

    return written


def _write_stop_time_updates(
    conn,
    trip:          TripUpdate,
    trip_update_id: str,
    fetch_log_id:  str,
    fetched_at:    datetime,
) -> None:
    """Insère les stop_time_updates d'un trip."""

    for i, stu in enumerate(trip.stop_time_update):
        stu_pk = f"{trip_update_id}_{i}_{stu.stop_id or stu.stop_sequence or i}"

        try:
            conn.execute("""
                INSERT OR IGNORE INTO bronze.stop_time_updates (
                    id, trip_update_id, fetch_log_id, fetched_at,
                    stop_sequence, stop_id,
                    arrival_delay_s, arrival_time,
                    departure_delay_s, departure_time,
                    schedule_rel, delay_seconds, delay_minutes,
                    is_delayed, is_cancelled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                stu_pk,
                trip_update_id,
                fetch_log_id,
                fetched_at,
                stu.stop_sequence,
                stu.stop_id,
                stu.arrival.delay   if stu.arrival   else None,
                stu.arrival.time    if stu.arrival   else None,
                stu.departure.delay if stu.departure else None,
                stu.departure.time  if stu.departure else None,
                stu.schedule_relationship.value,
                stu.delay_seconds,
                stu.delay_minutes,
                stu.is_delayed,
                stu.is_cancelled,
            ])
        except Exception as exc:
            logger.warning(f"Erreur écriture stu {stu_pk}: {exc}")


# ---------------------------------------------------------------------------
# Écriture bronze.service_alerts
# ---------------------------------------------------------------------------

def _write_service_alerts(conn, result: FetchResult, fetch_log_id: str) -> int:
    """Insère toutes les ServiceAlerts d'un FetchResult."""
    written = 0

    for alert in result.service_alerts:
        alert_pk = f"{fetch_log_id}_{alert.entity_id}"

        # Première période active pour simplifier le stockage
        period_start = alert.active_period[0].start if alert.active_period else None
        period_end   = alert.active_period[0].end   if alert.active_period else None

        try:
            conn.execute("""
                INSERT OR IGNORE INTO bronze.service_alerts (
                    id, fetch_log_id, fetched_at, entity_id,
                    cause, effect, header_text, description_text,
                    active_period_start, active_period_end,
                    is_active_now, is_cancellation, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                alert_pk,
                fetch_log_id,
                result.fetched_at,
                alert.entity_id,
                alert.cause.value,
                alert.effect.value,
                alert.header_text,
                alert.description_text,
                period_start,
                period_end,
                alert.is_active_now,
                alert.is_cancellation,
                _to_json(alert),
            ])
            written += 1

        except Exception as exc:
            logger.warning(f"Erreur écriture alert {alert.entity_id}: {exc}")

    return written


# ---------------------------------------------------------------------------
# Écriture silver.data_quality_checks
# ---------------------------------------------------------------------------

def _write_quality_checks(conn, result: FetchResult, fetch_log_id: str, feed_type: str) -> None:
    """
    Exécute et persiste les contrôles qualité automatiques sur chaque fetch.
    Ces checks alimentent le Data Quality Score affiché dans le dashboard
    et la page de gouvernance.

    Checks implémentés :
      - freshness   : la donnée a moins de 5 minutes
      - availability: le flux répond en HTTP 200
      - completeness: au moins 100 entités parsées (seuil minimal)
      - parse_validity: taux d'erreur de parsing < 5%
    """
    now = datetime.now(UTC)

    checks = [
        {
            "name":     "freshness",
            "passed":   (result.data_freshness_seconds or 999) < 300,
            "score":    max(0.0, 1.0 - (result.data_freshness_seconds or 0) / 300),
            "expected": "< 300s",
            "actual":   f"{result.data_freshness_seconds:.0f}s" if result.data_freshness_seconds else "N/A",
            "details":  "La donnée doit avoir moins de 5 minutes pour être considérée fraîche",
        },
        {
            "name":     "availability",
            "passed":   result.http_status == 200,
            "score":    1.0 if result.http_status == 200 else 0.0,
            "expected": "HTTP 200",
            "actual":   f"HTTP {result.http_status}",
            "details":  "Le flux doit répondre HTTP 200",
        },
        {
            "name":     "completeness",
            "passed":   result.entity_count >= 100,
            "score":    min(1.0, result.entity_count / 500),
            "expected": ">= 100 entités",
            "actual":   f"{result.entity_count} entités",
            "details":  "Un flux vide ou quasi-vide signale une anomalie côté SNCF",
        },
        {
            "name":     "parse_validity",
            "passed":   (result.error_count / max(result.entity_count, 1)) < 0.05,
            "score":    max(0.0, 1.0 - result.error_count / max(result.entity_count, 1)),
            "expected": "< 5% d'erreurs",
            "actual":   f"{result.error_count}/{result.entity_count} erreurs",
            "details":  "Taux d'erreurs de parsing Protobuf",
        },
    ]

    for check in checks:
        conn.execute("""
            INSERT OR IGNORE INTO silver.data_quality_checks (
                id, fetch_log_id, checked_at, check_name, source,
                passed, score, expected_value, actual_value, details
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            _new_id(),
            fetch_log_id,
            now,
            check["name"],
            feed_type,
            check["passed"],
            check["score"],
            check["expected"],
            check["actual"],
            check["details"],
        ])


# ---------------------------------------------------------------------------
# Mise à jour silver.delay_events (retards significatifs)
# ---------------------------------------------------------------------------

def _write_delay_events(conn, result: FetchResult, fetch_log_id: str) -> None:
    """
    Crée des événements dans silver.delay_events pour les retards > 5 min.
    Ces événements seront lus par le LangChain anomaly detector (V2).
    """
    for trip in result.trip_updates:
        if trip.max_delay_minutes and trip.max_delay_minutes >= 5:
            # Trouve l'arrêt avec le plus grand retard
            worst_stop = max(
                (s for s in trip.stop_time_update if s.delay_seconds),
                key=lambda s: s.delay_seconds or 0,
                default=None,
            )
            conn.execute("""
                INSERT OR IGNORE INTO silver.delay_events (
                    id, trip_id, route_id, start_date,
                    detected_at, delay_minutes, stop_id, severity
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                _new_id(),
                trip.trip.trip_id,
                trip.trip.route_id,
                trip.trip.start_date,
                result.fetched_at,
                trip.max_delay_minutes,
                worst_stop.stop_id if worst_stop else None,
                _delay_severity(trip.max_delay_minutes),
            ])


# ---------------------------------------------------------------------------
# Mise à jour gold.data_catalog_metadata
# ---------------------------------------------------------------------------

def _update_catalog(conn, result: FetchResult, source_id: str) -> None:
    """
    Met à jour le statut live de la source dans le Data Catalog.
    Visible dans la page Gouvernance du dashboard.
    """
    status = (
        "healthy"  if result.quality_score >= 0.9 else
        "degraded" if result.quality_score >= 0.6 else
        "down"
    )
    conn.execute("""
        UPDATE gold.data_catalog_metadata
        SET
            last_fetch_at      = ?,
            last_quality_score = ?,
            current_status     = ?,
            updated_at         = NOW()
        WHERE source_id = ?
    """, [result.fetched_at, result.quality_score, status, source_id])


# ---------------------------------------------------------------------------
# Fonction principale publique
# ---------------------------------------------------------------------------

def write_fetch_results(
    trip_result:   FetchResult,
    alert_result:  FetchResult,
) -> dict:
    """
    Point d'entrée principal du writer.
    Persiste les deux FetchResults (trips + alerts) dans DuckDB
    en une seule transaction.

    Args:
        trip_result:  FetchResult du flux GTFS-RT Trip Updates
        alert_result: FetchResult du flux GTFS-RT Service Alerts

    Returns:
        Dictionnaire de stats d'écriture pour le monitoring.
    """
    stats = {
        "trips_written":   0,
        "alerts_written":  0,
        "errors":          0,
    }

    with get_db() as conn:
        try:
            # --- Trip Updates ---
            trip_fetch_id = _write_fetch_log(conn, trip_result, "trip_updates")
            stats["trips_written"] = _write_trip_updates(conn, trip_result, trip_fetch_id)
            _write_quality_checks(conn, trip_result, trip_fetch_id, "trip_updates")
            _write_delay_events(conn, trip_result, trip_fetch_id)
            _update_catalog(conn, trip_result, "gtfs-rt-trip-updates")

            # --- Service Alerts ---
            alert_fetch_id = _write_fetch_log(conn, alert_result, "service_alerts")
            stats["alerts_written"] = _write_service_alerts(conn, alert_result, alert_fetch_id)
            _write_quality_checks(conn, alert_result, alert_fetch_id, "service_alerts")
            _update_catalog(conn, alert_result, "gtfs-rt-service-alerts")

            logger.success(
                f"DuckDB ← {stats['trips_written']} trips | "
                f"{stats['alerts_written']} alerts | "
                f"quality TU={trip_result.quality_score:.0%} "
                f"SA={alert_result.quality_score:.0%}"
            )

        except Exception as exc:
            logger.error(f"Erreur écriture DuckDB : {exc}")
            stats["errors"] += 1
            raise

    return stats