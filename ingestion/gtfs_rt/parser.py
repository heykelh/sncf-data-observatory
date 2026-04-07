"""
GTFS-RT Parser
==============
Transforme les bytes protobuf bruts du flux SNCF en objets Pydantic typés.
Séparé du fetcher pour être testable indépendamment et réutilisable (LangChain, dbt).
"""

import time
from datetime import datetime, timezone
from typing import Optional

from google.transit import gtfs_realtime_pb2
from loguru import logger

from ingestion.gtfs_rt.models import (
    AlertCause,
    AlertEffect,
    AlertPeriod,
    FetchResult,
    ServiceAlert,
    StopScheduleRelationship,
    StopTimeEvent,
    StopTimeUpdate,
    TranslatedString,
    TripDescriptor,
    TripScheduleRelationship,
    TripUpdate,
)


# ---------------------------------------------------------------------------
# Helpers de conversion protobuf → Python
# ---------------------------------------------------------------------------

def _pb_timestamp_to_datetime(ts: int) -> Optional[datetime]:
    """Convertit un timestamp UNIX protobuf en datetime UTC."""
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    except (OSError, OverflowError, ValueError):
        return None


def _pb_str(translated_string) -> Optional[str]:
    """Extrait le texte français (ou premier disponible) d'un TranslatedString protobuf."""
    if not translated_string or not translated_string.translation:
        return None
    for t in translated_string.translation:
        if t.language in ("fr", ""):
            return t.text or None
    # Fallback : premier texte disponible
    return translated_string.translation[0].text or None


def _parse_schedule_relationship_trip(value: int) -> TripScheduleRelationship:
    mapping = {
        0: TripScheduleRelationship.SCHEDULED,
        1: TripScheduleRelationship.SKIPPED,
        2: TripScheduleRelationship.UNSCHEDULED,
    }
    return mapping.get(value, TripScheduleRelationship.SCHEDULED)


def _parse_schedule_relationship_stop(value: int) -> StopScheduleRelationship:
    mapping = {
        0: StopScheduleRelationship.SCHEDULED,
        1: StopScheduleRelationship.SKIPPED,
        2: StopScheduleRelationship.NO_DATA,
    }
    return mapping.get(value, StopScheduleRelationship.SCHEDULED)


def _parse_alert_cause(value: int) -> AlertCause:
    mapping = {
        0:  AlertCause.UNKNOWN_CAUSE,
        1:  AlertCause.OTHER_CAUSE,
        2:  AlertCause.TECHNICAL_PROBLEM,
        3:  AlertCause.STRIKE,
        4:  AlertCause.DEMONSTRATION,
        5:  AlertCause.ACCIDENT,
        6:  AlertCause.HOLIDAY,
        7:  AlertCause.WEATHER,
        8:  AlertCause.MAINTENANCE,
        9:  AlertCause.CONSTRUCTION,
        10: AlertCause.POLICE_ACTIVITY,
        11: AlertCause.MEDICAL_EMERGENCY,
    }
    return mapping.get(value, AlertCause.UNKNOWN_CAUSE)


def _parse_alert_effect(value: int) -> AlertEffect:
    mapping = {
        0: AlertEffect.NO_SERVICE,
        1: AlertEffect.REDUCED_SERVICE,
        2: AlertEffect.SIGNIFICANT_DELAYS,
        3: AlertEffect.DETOUR,
        4: AlertEffect.ADDITIONAL_SERVICE,
        5: AlertEffect.MODIFIED_SERVICE,
        6: AlertEffect.OTHER_EFFECT,
        7: AlertEffect.UNKNOWN_EFFECT,
        8: AlertEffect.STOP_MOVED,
    }
    return mapping.get(value, AlertEffect.UNKNOWN_EFFECT)


# ---------------------------------------------------------------------------
# Parsers par entité
# ---------------------------------------------------------------------------

def _parse_stop_time_event(pb_event) -> Optional[StopTimeEvent]:
    """Parse un StopTimeEvent protobuf (arrivée ou départ)."""
    if pb_event is None:
        return None
    delay = pb_event.delay if pb_event.HasField("delay") else None
    time_val = _pb_timestamp_to_datetime(pb_event.time) if pb_event.HasField("time") else None
    if delay is None and time_val is None:
        return None
    return StopTimeEvent(delay=delay, time=time_val)


def _parse_stop_time_update(pb_stu) -> StopTimeUpdate:
    """Parse un StopTimeUpdate protobuf."""
    arrival   = _parse_stop_time_event(pb_stu.arrival)   if pb_stu.HasField("arrival")   else None
    departure = _parse_stop_time_event(pb_stu.departure) if pb_stu.HasField("departure") else None

    return StopTimeUpdate(
        stop_sequence=pb_stu.stop_sequence or None,
        stop_id=pb_stu.stop_id or None,
        arrival=arrival,
        departure=departure,
        schedule_relationship=_parse_schedule_relationship_stop(pb_stu.schedule_relationship),
    )


def _parse_trip_descriptor(pb_trip) -> TripDescriptor:
    """Parse un TripDescriptor protobuf."""
    return TripDescriptor(
        trip_id=pb_trip.trip_id or None,
        route_id=pb_trip.route_id or None,
        direction_id=pb_trip.direction_id if pb_trip.HasField("direction_id") else None,
        start_time=pb_trip.start_time or None,
        start_date=pb_trip.start_date or None,
        schedule_relationship=_parse_schedule_relationship_trip(pb_trip.schedule_relationship),
    )


def _parse_trip_update(pb_entity) -> Optional[TripUpdate]:
    """
    Parse une entité TripUpdate complète depuis le protobuf.
    Retourne None si l'entité est invalide ou incomplète.
    """
    try:
        pb_tu = pb_entity.trip_update
        trip  = _parse_trip_descriptor(pb_tu.trip)
        stop_time_updates = [
            _parse_stop_time_update(stu) for stu in pb_tu.stop_time_update
        ]
        timestamp = _pb_timestamp_to_datetime(pb_tu.timestamp) if pb_tu.HasField("timestamp") else None

        return TripUpdate(
            entity_id=pb_entity.id,
            trip=trip,
            stop_time_update=stop_time_updates,
            timestamp=timestamp,
        )
    except Exception as exc:
        logger.warning(f"Impossible de parser TripUpdate entity_id={pb_entity.id}: {exc}")
        return None


def _parse_service_alert(pb_entity) -> Optional[ServiceAlert]:
    """
    Parse une entité ServiceAlert complète depuis le protobuf.
    Retourne None si l'entité est invalide.
    """
    try:
        pb_alert = pb_entity.alert
        active_periods = [
            AlertPeriod(
                start=_pb_timestamp_to_datetime(p.start) if p.start else None,
                end=_pb_timestamp_to_datetime(p.end)   if p.end   else None,
            )
            for p in pb_alert.active_period
        ]

        return ServiceAlert(
            entity_id=pb_entity.id,
            active_period=active_periods,
            cause=_parse_alert_cause(pb_alert.cause),
            effect=_parse_alert_effect(pb_alert.effect),
            header_text=_pb_str(pb_alert.header_text),
            description_text=_pb_str(pb_alert.description_text),
        )
    except Exception as exc:
        logger.warning(f"Impossible de parser ServiceAlert entity_id={pb_entity.id}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Fonction principale de parsing
# ---------------------------------------------------------------------------

def parse_feed(
    raw_bytes:  bytes,
    source_url: str,
    http_status: int = 200,
    feed_type:  str = "trip_updates",  # "trip_updates" | "service_alerts"
) -> FetchResult:
    """
    Point d'entrée principal du parser.

    Prend les bytes bruts d'un flux GTFS-RT et retourne un FetchResult
    contenant toutes les entités parsées + les métriques de qualité.

    Args:
        raw_bytes:   Contenu binaire du flux protobuf
        source_url:  URL source (pour la traçabilité / gouvernance)
        http_status: Code HTTP de la réponse (pour le quality score)
        feed_type:   Type de flux à parser

    Returns:
        FetchResult avec trip_updates ou service_alerts remplis + métriques
    """
    t_start = time.perf_counter()

    result = FetchResult(
        source_url=source_url,
        http_status=http_status,
    )

    if http_status != 200 or not raw_bytes:
        logger.error(f"Flux invalide — HTTP {http_status}, bytes={len(raw_bytes or b'')}")
        result.error_count = 1
        return result

    # --- Désérialisation protobuf ---
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(raw_bytes)
    except Exception as exc:
        logger.error(f"Erreur protobuf parsing: {exc}")
        result.error_count = 1
        return result

    # Timestamp du flux (fraîcheur de la donnée)
    if feed.header.HasField("timestamp"):
        result.feed_timestamp = _pb_timestamp_to_datetime(feed.header.timestamp)

    result.entity_count = len(feed.entity)
    logger.info(f"Flux reçu — {result.entity_count} entités | feed_ts={result.feed_timestamp}")

    # --- Parsing des entités ---
    trip_updates:   list[TripUpdate]   = []
    service_alerts: list[ServiceAlert] = []
    error_count  = 0
    skip_count   = 0

    for entity in feed.entity:
        try:
            if entity.HasField("trip_update") and feed_type == "trip_updates":
                parsed = _parse_trip_update(entity)
                if parsed:
                    trip_updates.append(parsed)
                else:
                    error_count += 1

            elif entity.HasField("alert") and feed_type == "service_alerts":
                parsed = _parse_service_alert(entity)
                if parsed:
                    service_alerts.append(parsed)
                else:
                    error_count += 1

            else:
                skip_count += 1

        except Exception as exc:
            logger.warning(f"Entité ignorée ({entity.id}): {exc}")
            error_count += 1

    # --- Résultat final ---
    t_end = time.perf_counter()

    result.trip_updates    = trip_updates
    result.service_alerts  = service_alerts
    result.error_count     = error_count
    result.skipped_count   = skip_count
    result.parse_duration_ms = round((t_end - t_start) * 1000, 2)

    logger.success(
        f"Parsing terminé — "
        f"{len(trip_updates)} trip_updates | "
        f"{len(service_alerts)} alerts | "
        f"{error_count} erreurs | "
        f"{result.parse_duration_ms}ms | "
        f"quality_score={result.quality_score}"
    )

    return result