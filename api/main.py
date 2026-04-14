"""
FastAPI — SNCF Data Quality Observatory
========================================
Point d'entrée principal de l'API REST.

Endpoints disponibles :
  GET  /                        → Info API
  GET  /health                  → Santé du pipeline + DB stats
  GET  /status                  → Dernier cycle d'ingestion
  GET  /trips/live              → Trains en circulation avec retards
  GET  /trips/{trip_id}         → Détail d'un train
  GET  /alerts/active           → Alertes de perturbation actives
  GET  /kpi/punctuality         → Taux de ponctualité par heure
  GET  /kpi/quality             → Scores de qualité par source
  GET  /kpi/history/{train_type}→ Tendance historique de régularité
  GET  /governance/catalog      → Data Catalog complet
  GET  /governance/quality      → Derniers contrôles qualité
  WS   /ws/live                 → WebSocket — feed live toutes les 2 min

Lancer :
    uvicorn api.main:app --reload --port 8000
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel

from ingestion.gtfs_rt.fetcher import (
    get_cycle_count,
    get_last_alert_result,
    get_last_trip_result,
    start_scheduler,
    stop_scheduler,
)
from ingestion.regularity.loader import (
    get_punctuality_trend,
    get_regularity_summary,
    load_all_regularity_data,
)
from storage.database import get_db, get_db_size_mb, get_latest_fetch_info, get_table_stats, init_db

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API démarrage — initialisation")

    # Init DB (crée les tables si elles n'existent pas)
    try:
        init_db()
        logger.info("DB initialisée")
    except Exception as e:
        logger.warning(f"DB init : {e}")

    # Lance le scheduler GTFS-RT
    logger.info("Lancement du scheduler GTFS-RT")
    start_scheduler()

    # Chargement initial des données si tables vides
    try:
        with get_db(read_only=True) as conn:
            try:
                count = conn.execute(
                    "SELECT COUNT(*) FROM bronze.regularity_raw"
                ).fetchone()[0]
            except Exception:
                count = 0

        if count == 0:
            logger.info("Données absentes — chargement initial en arrière-plan...")
            async def init_data():
                try:
                    await load_all_regularity_data()
                    logger.success("Données régularité chargées")
                except Exception as e:
                    logger.warning(f"Régularité : {e}")
                try:
                    from ingestion.gtfs_static.loader import load_gtfs_stops
                    await load_gtfs_stops()
                    logger.success("Référentiel gares chargé")
                except Exception as e:
                    logger.warning(f"GTFS stops : {e}")
            asyncio.create_task(init_data())
        else:
            logger.info(f"Données déjà en base ({count} enregistrements)")
    except Exception as e:
        logger.warning(f"Init data : {e}")

    yield

    logger.info("API arrêt — shutdown scheduler")
    stop_scheduler()


# ---------------------------------------------------------------------------
# Application FastAPI
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SNCF Data Quality Observatory",
    description=(
        "API temps réel de monitoring de la qualité des données ferroviaires SNCF. "
        "Ingestion GTFS-RT toutes les 2 minutes — TGV, IC, TER, OUIGO. "
        "Gouvernance des données : Data Catalog, Quality Score, Data Lineage."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Schémas Pydantic
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status:      str
    pipeline:    str
    cycle_count: int
    db_size_mb:  float
    table_stats: dict
    last_fetch:  Optional[dict]
    timestamp:   str


class TripSummary(BaseModel):
    entity_id:         str
    trip_id:           Optional[str]
    route_id:          Optional[str]
    start_time:        Optional[str]
    start_date:        Optional[str]
    max_delay_minutes: Optional[float]
    has_cancellation:  bool
    affected_stops:    int
    schedule_rel:      str


class AlertSummary(BaseModel):
    entity_id:   str
    cause:       str
    effect:      str
    header_text: Optional[str]
    is_active:   bool
    is_cancel:   bool


class LiveFeedResponse(BaseModel):
    fetched_at:      str
    total_trips:     int
    delayed_trips:   int
    cancelled_trips: int
    on_time_trips:   int
    quality_score:   float
    freshness_s:     Optional[float]
    trips:           list[TripSummary]


class AlertsFeedResponse(BaseModel):
    fetched_at:    str
    total_alerts:  int
    active_alerts: int
    quality_score: float
    alerts:        list[AlertSummary]


class KpiHourlyRow(BaseModel):
    hour_bucket:       str
    total_trips:       int
    delayed_trips:     int
    cancelled_trips:   int
    punctuality_rate:  Optional[float]
    avg_delay_minutes: Optional[float]


class QualityCheckRow(BaseModel):
    checked_at:     str
    check_name:     str
    source:         str
    passed:         bool
    score:          Optional[float]
    expected_value: Optional[str]
    actual_value:   Optional[str]


class CatalogEntry(BaseModel):
    source_id:          str
    source_name:        str
    source_url:         Optional[str]
    feed_type:          Optional[str]
    owner:              Optional[str]
    sla_freshness_s:    Optional[int]
    sla_availability:   Optional[float]
    last_fetch_at:      Optional[str]
    last_quality_score: Optional[float]
    current_status:     Optional[str]
    description:        Optional[str]


# ---------------------------------------------------------------------------
# Routes — Info & Health
# ---------------------------------------------------------------------------

@app.get("/", tags=["Info"])
async def root():
    return {
        "name":        "SNCF Data Quality Observatory",
        "version":     "1.0.0",
        "author":      "Michel DUPONT — github.com/heykelh",
        "description": "Pipeline temps réel de qualité des données ferroviaires SNCF",
        "docs":        "/docs",
        "sources": {
            "gtfs_rt_trip_updates":   "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates",
            "gtfs_rt_service_alerts": "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-service-alerts",
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health", response_model=HealthResponse, tags=["Info"])
async def health():
    last = get_last_trip_result()
    pipeline_status = "healthy"
    if last is None:
        pipeline_status = "starting"
    elif last.data_freshness_seconds and last.data_freshness_seconds > 600:
        pipeline_status = "degraded"
    elif not last.success:
        pipeline_status = "error"

    return HealthResponse(
        status="ok",
        pipeline=pipeline_status,
        cycle_count=get_cycle_count(),
        db_size_mb=get_db_size_mb(),
        table_stats=get_table_stats(),
        last_fetch=get_latest_fetch_info(),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/status", tags=["Info"])
async def status():
    trip_result  = get_last_trip_result()
    alert_result = get_last_alert_result()

    if not trip_result:
        return {"status": "no_data", "message": "Aucun fetch effectué depuis le démarrage"}

    return {
        "cycle_count":   get_cycle_count(),
        "last_fetch_at": trip_result.fetched_at.isoformat(),
        "trip_updates": {
            "entity_count":  trip_result.entity_count,
            "error_count":   trip_result.error_count,
            "quality_score": trip_result.quality_score,
            "freshness_s":   trip_result.data_freshness_seconds,
        },
        "service_alerts": {
            "entity_count":  alert_result.entity_count if alert_result else 0,
            "quality_score": alert_result.quality_score if alert_result else 0,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Routes — Trips
# ---------------------------------------------------------------------------

@app.get("/trips/live", response_model=LiveFeedResponse, tags=["Temps réel"])
async def trips_live(
    delayed_only: bool  = Query(False, description="Filtre : retards uniquement"),
    min_delay:    float = Query(0.0,   description="Retard minimum en minutes"),
    limit:        int   = Query(100,   description="Nombre max de trips retournés", le=500),
):
    result = get_last_trip_result()
    if not result:
        return LiveFeedResponse(
            fetched_at=datetime.now(timezone.utc).isoformat(),
            total_trips=0, delayed_trips=0, cancelled_trips=0,
            on_time_trips=0, quality_score=0.0, freshness_s=None, trips=[],
        )

    trips = result.trip_updates
    if delayed_only:
        trips = [t for t in trips if t.max_delay_minutes and t.max_delay_minutes > 0]
    if min_delay > 0:
        trips = [t for t in trips if t.max_delay_minutes and t.max_delay_minutes >= min_delay]

    delayed   = [t for t in result.trip_updates if t.max_delay_minutes and t.max_delay_minutes > 1]
    cancelled = [t for t in result.trip_updates if t.has_cancellation]
    on_time   = len(result.trip_updates) - len(delayed) - len(cancelled)

    return LiveFeedResponse(
        fetched_at=result.fetched_at.isoformat(),
        total_trips=len(result.trip_updates),
        delayed_trips=len(delayed),
        cancelled_trips=len(cancelled),
        on_time_trips=max(0, on_time),
        quality_score=result.quality_score,
        freshness_s=result.data_freshness_seconds,
        trips=[
            TripSummary(
                entity_id=t.entity_id,
                trip_id=t.trip.trip_id,
                route_id=t.trip.route_id,
                start_time=t.trip.start_time,
                start_date=t.trip.start_date,
                max_delay_minutes=t.max_delay_minutes,
                has_cancellation=t.has_cancellation,
                affected_stops=t.affected_stops_count,
                schedule_rel=t.trip.schedule_relationship.value,
            )
            for t in trips[:limit]
        ],
    )


@app.get("/trips/{trip_id}", tags=["Temps réel"])
async def trip_detail(trip_id: str):
    result = get_last_trip_result()
    if not result:
        raise HTTPException(status_code=503, detail="Aucune donnée disponible")

    trip = next(
        (t for t in result.trip_updates if t.trip.trip_id == trip_id or t.entity_id == trip_id),
        None
    )
    if not trip:
        raise HTTPException(status_code=404, detail=f"Trip '{trip_id}' non trouvé")

    return {
        "entity_id":         trip.entity_id,
        "trip_id":           trip.trip.trip_id,
        "route_id":          trip.trip.route_id,
        "start_time":        trip.trip.start_time,
        "start_date":        trip.trip.start_date,
        "schedule_rel":      trip.trip.schedule_relationship.value,
        "max_delay_minutes": trip.max_delay_minutes,
        "has_cancellation":  trip.has_cancellation,
        "affected_stops":    trip.affected_stops_count,
        "stop_time_updates": [
            {
                "stop_id":       s.stop_id,
                "stop_sequence": s.stop_sequence,
                "delay_minutes": s.delay_minutes,
                "is_delayed":    s.is_delayed,
                "is_cancelled":  s.is_cancelled,
                "schedule_rel":  s.schedule_relationship.value,
            }
            for s in trip.stop_time_update
        ],
        "fetched_at": result.fetched_at.isoformat(),
    }


# ---------------------------------------------------------------------------
# Routes — Alerts
# ---------------------------------------------------------------------------

@app.get("/alerts/active", response_model=AlertsFeedResponse, tags=["Temps réel"])
async def alerts_active(
    cancellations_only: bool = Query(False, description="Filtre : suppressions uniquement"),
    limit:              int  = Query(50,    description="Nombre max d'alertes", le=500),
):
    result = get_last_alert_result()
    if not result:
        return AlertsFeedResponse(
            fetched_at=datetime.now(timezone.utc).isoformat(),
            total_alerts=0, active_alerts=0, quality_score=0.0, alerts=[],
        )

    alerts = [a for a in result.service_alerts if a.is_active_now]
    if cancellations_only:
        alerts = [a for a in alerts if a.is_cancellation]

    return AlertsFeedResponse(
        fetched_at=result.fetched_at.isoformat(),
        total_alerts=len(result.service_alerts),
        active_alerts=len([a for a in result.service_alerts if a.is_active_now]),
        quality_score=result.quality_score,
        alerts=[
            AlertSummary(
                entity_id=a.entity_id,
                cause=a.cause.value,
                effect=a.effect.value,
                header_text=a.header_text,
                is_active=a.is_active_now,
                is_cancel=a.is_cancellation,
            )
            for a in alerts[:limit]
        ],
    )


# ---------------------------------------------------------------------------
# Routes — KPIs
# ---------------------------------------------------------------------------

@app.get("/kpi/punctuality", tags=["KPIs"])
async def kpi_punctuality(
    hours: int = Query(24, description="Fenêtre de temps en heures", le=168),
):
    with get_db(read_only=True) as conn:
        rows = conn.execute("""
            SELECT
                DATE_TRUNC('hour', fetched_at)          AS hour_bucket,
                COUNT(*)                                AS total_trips,
                SUM(CASE WHEN max_delay_minutes > 1 THEN 1 ELSE 0 END)  AS delayed,
                SUM(CASE WHEN has_cancellation THEN 1 ELSE 0 END)        AS cancelled,
                ROUND(
                    100.0 * SUM(CASE WHEN max_delay_minutes <= 1 AND NOT has_cancellation THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 2
                )                                       AS punctuality_rate,
                ROUND(AVG(CASE WHEN max_delay_minutes > 0 THEN max_delay_minutes END), 2) AS avg_delay
            FROM bronze.trip_updates
            WHERE fetched_at >= NOW() - INTERVAL (?) HOUR
            GROUP BY hour_bucket
            ORDER BY hour_bucket DESC
        """, [hours]).fetchall()

    return {
        "window_hours": hours,
        "data": [
            {
                "hour":             str(row[0]),
                "total_trips":      row[1],
                "delayed_trips":    row[2],
                "cancelled_trips":  row[3],
                "punctuality_rate": row[4],
                "avg_delay_min":    row[5],
            }
            for row in rows
        ],
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/kpi/quality", tags=["KPIs"])
async def kpi_quality(
    hours: int = Query(24, description="Fenêtre de temps en heures", le=168),
):
    with get_db(read_only=True) as conn:
        rows = conn.execute("""
            SELECT
                source,
                check_name,
                COUNT(*)                        AS total_checks,
                SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_checks,
                ROUND(AVG(score), 4)            AS avg_score,
                ROUND(
                    100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2
                )                               AS pass_rate
            FROM silver.data_quality_checks
            WHERE checked_at >= NOW() - INTERVAL (?) HOUR
            GROUP BY source, check_name
            ORDER BY source, check_name
        """, [hours]).fetchall()

    by_source: dict = {}
    for row in rows:
        source = row[0]
        if source not in by_source:
            by_source[source] = {"checks": {}, "overall_score": 0.0}
        by_source[source]["checks"][row[1]] = {
            "total":     row[2],
            "passed":    row[3],
            "avg_score": row[4],
            "pass_rate": row[5],
        }

    for source, data in by_source.items():
        scores = [c["avg_score"] for c in data["checks"].values() if c["avg_score"] is not None]
        data["overall_score"] = round(sum(scores) / len(scores), 4) if scores else 0.0

    return {
        "window_hours": hours,
        "sources":      by_source,
        "computed_at":  datetime.now(timezone.utc).isoformat(),
    }


@app.get("/kpi/history/{train_type}", tags=["KPIs"])
async def kpi_history(
    train_type: str,
    year_from:  int           = Query(2020, description="Année de début"),
    region:     Optional[str] = Query(None, description="Filtre région (TER uniquement)"),
    axe:        Optional[str] = Query(None, description="Filtre axe (TGV/IC uniquement)"),
):
    valid_types = {"tgv", "tgv_axe", "ter", "transilien", "intercites"}
    if train_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"train_type invalide. Valeurs acceptées : {valid_types}"
        )

    data = get_punctuality_trend(
        train_type=train_type,
        year_from=year_from,
        region=region,
        axe=axe,
    )

    return {
        "train_type": train_type,
        "year_from":  year_from,
        "filters":    {"region": region, "axe": axe},
        "count":      len(data),
        "data":       data,
    }


@app.get("/kpi/delays/top", tags=["KPIs"])
async def kpi_top_delays(
    limit: int = Query(10, description="Nombre de trains à retourner", le=50),
    hours: int = Query(24, description="Fenêtre en heures", le=168),
):
    with get_db(read_only=True) as conn:
        rows = conn.execute("""
            SELECT
                trip_id,
                route_id,
                start_date,
                MAX(max_delay_minutes)  AS max_delay,
                MIN(fetched_at)         AS first_seen,
                MAX(fetched_at)         AS last_seen,
                SUM(CASE WHEN has_cancellation THEN 1 ELSE 0 END) AS cancel_count
            FROM bronze.trip_updates
            WHERE fetched_at >= NOW() - INTERVAL (?) HOUR
              AND max_delay_minutes IS NOT NULL
            GROUP BY trip_id, route_id, start_date
            ORDER BY max_delay DESC
            LIMIT ?
        """, [hours, limit]).fetchall()

    return {
        "window_hours": hours,
        "data": [
            {
                "trip_id":       row[0],
                "route_id":      row[1],
                "start_date":    row[2],
                "max_delay_min": row[3],
                "first_seen":    str(row[4]),
                "last_seen":     str(row[5]),
                "cancelled":     row[6] > 0,
            }
            for row in rows
        ],
    }


# ---------------------------------------------------------------------------
# Routes — Référentiel gares
# ---------------------------------------------------------------------------

@app.get("/stops/resolve", tags=["Référentiel"])
async def resolve_stops(
    uic_codes: str = Query(..., description="Codes UIC séparés par virgule")
):
    codes = [c.strip() for c in uic_codes.split(",") if c.strip()]
    if not codes:
        return {"stops": {}}
    placeholders = ",".join(["?" for _ in codes])
    with get_db(read_only=True) as conn:
        try:
            rows = conn.execute(f"""
                SELECT uic_code, stop_name, stop_lat, stop_lon
                FROM bronze.stops
                WHERE uic_code IN ({placeholders})
                GROUP BY uic_code, stop_name, stop_lat, stop_lon
            """, codes).fetchall()
        except Exception as e:
            logger.warning(f"Erreur resolve_stops: {e}")
            return {"stops": {}}
    return {"stops": {r[0]: {"name": r[1], "lat": r[2], "lon": r[3]} for r in rows}}


# ---------------------------------------------------------------------------
# Routes — Gouvernance
# ---------------------------------------------------------------------------

@app.get("/governance/catalog", tags=["Gouvernance"])
async def governance_catalog():
    with get_db(read_only=True) as conn:
        rows = conn.execute("""
            SELECT
                source_id, source_name, source_url, feed_type,
                owner, sla_freshness_s, sla_availability,
                last_fetch_at, last_quality_score, current_status,
                description
            FROM gold.data_catalog_metadata
            ORDER BY source_id
        """).fetchall()

    return {
        "sources": [
            CatalogEntry(
                source_id=row[0],
                source_name=row[1],
                source_url=row[2],
                feed_type=row[3],
                owner=row[4],
                sla_freshness_s=row[5],
                sla_availability=row[6],
                last_fetch_at=str(row[7]) if row[7] else None,
                last_quality_score=row[8],
                current_status=row[9],
                description=row[10],
            )
            for row in rows
        ],
        "total":        len(rows),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/governance/quality", tags=["Gouvernance"])
async def governance_quality(
    hours: int = Query(24, description="Fenêtre en heures"),
):
    with get_db(read_only=True) as conn:
        rows = conn.execute("""
            SELECT
                checked_at, check_name, source,
                passed, score, expected_value, actual_value
            FROM silver.data_quality_checks
            WHERE checked_at >= NOW() - INTERVAL (?) HOUR
            ORDER BY checked_at DESC
            LIMIT 200
        """, [hours]).fetchall()

    return {
        "window_hours": hours,
        "checks": [
            QualityCheckRow(
                checked_at=str(row[0]),
                check_name=row[1],
                source=row[2],
                passed=row[3],
                score=row[4],
                expected_value=row[5],
                actual_value=row[6],
            )
            for row in rows
        ],
        "total":        len(rows),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/governance/regularity/summary", tags=["Gouvernance"])
async def governance_regularity_summary():
    return {
        "summary":      get_regularity_summary(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/governance/delay-events", tags=["Gouvernance"])
async def governance_delay_events(
    hours:    int            = Query(24,   description="Fenêtre en heures"),
    severity: Optional[str] = Query(None, description="Filtre : minor|moderate|severe|critical"),
):
    query  = """
        SELECT id, trip_id, route_id, start_date,
               detected_at, delay_minutes, stop_id, severity
        FROM silver.delay_events
        WHERE detected_at >= NOW() - INTERVAL (?) HOUR
    """
    params = [hours]
    if severity:
        query += " AND severity = ?"
        params.append(severity)
    query += " ORDER BY delay_minutes DESC LIMIT 100"

    with get_db(read_only=True) as conn:
        rows = conn.execute(query, params).fetchall()

    return {
        "window_hours": hours,
        "events": [
            {
                "id":            row[0],
                "trip_id":       row[1],
                "route_id":      row[2],
                "start_date":    row[3],
                "detected_at":   str(row[4]),
                "delay_minutes": row[5],
                "stop_id":       row[6],
                "severity":      row[7],
            }
            for row in rows
        ],
        "total": len(rows),
    }


# ---------------------------------------------------------------------------
# WebSocket — Live feed
# ---------------------------------------------------------------------------

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        logger.info(f"WebSocket connecté — {len(self.active)} client(s)")

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
        logger.info(f"WebSocket déconnecté — {len(self.active)} client(s)")

    async def broadcast(self, data: dict):
        if not self.active:
            return
        message      = json.dumps(data, default=str)
        disconnected = []
        for ws in self.active:
            try:
                await ws.send_text(message)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            if ws in self.active:
                self.active.remove(ws)


manager = ConnectionManager()


@app.websocket("/ws/live")
async def websocket_live(ws: WebSocket):
    await manager.connect(ws)
    trip_result  = get_last_trip_result()
    alert_result = get_last_alert_result()

    if trip_result:
        delayed   = sum(1 for t in trip_result.trip_updates if t.max_delay_minutes and t.max_delay_minutes > 1)
        cancelled = sum(1 for t in trip_result.trip_updates if t.has_cancellation)
        await ws.send_text(json.dumps({
            "type":            "snapshot",
            "cycle":           get_cycle_count(),
            "fetched_at":      trip_result.fetched_at.isoformat(),
            "total_trips":     len(trip_result.trip_updates),
            "delayed_trips":   delayed,
            "cancelled_trips": cancelled,
            "active_alerts":   len([
                a for a in (alert_result.service_alerts if alert_result else [])
                if a.is_active_now
            ]),
            "quality_score":   trip_result.quality_score,
            "freshness_s":     trip_result.data_freshness_seconds,
        }, default=str))

    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        manager.disconnect(ws)