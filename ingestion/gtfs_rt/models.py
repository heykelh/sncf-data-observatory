"""
GTFS-RT Pydantic Models
=======================
Représentation typée de toutes les entités issues des flux GTFS-RT SNCF.
Conçu pour être étendu facilement (LangChain agents, dbt, FastAPI schemas).
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TripScheduleRelationship(str, Enum):
    """Relation entre le trip temps réel et l'horaire théorique."""
    SCHEDULED   = "SCHEDULED"    # Circule selon horaire prévu
    SKIPPED     = "SKIPPED"      # Trip supprimé
    UNSCHEDULED = "UNSCHEDULED"  # Trip non prévu à l'horaire


class StopScheduleRelationship(str, Enum):
    """Relation entre un arrêt temps réel et l'horaire théorique."""
    SCHEDULED   = "SCHEDULED"
    SKIPPED     = "SKIPPED"      # Arrêt sauté
    NO_DATA     = "NO_DATA"      # Pas d'info temps réel pour cet arrêt


class AlertCause(str, Enum):
    """Cause d'une alerte de perturbation."""
    UNKNOWN_CAUSE        = "UNKNOWN_CAUSE"
    OTHER_CAUSE          = "OTHER_CAUSE"
    TECHNICAL_PROBLEM    = "TECHNICAL_PROBLEM"
    STRIKE               = "STRIKE"
    DEMONSTRATION        = "DEMONSTRATION"
    ACCIDENT             = "ACCIDENT"
    HOLIDAY              = "HOLIDAY"
    WEATHER              = "WEATHER"
    MAINTENANCE          = "MAINTENANCE"
    CONSTRUCTION         = "CONSTRUCTION"
    POLICE_ACTIVITY      = "POLICE_ACTIVITY"
    MEDICAL_EMERGENCY    = "MEDICAL_EMERGENCY"


class AlertEffect(str, Enum):
    """Effet d'une alerte sur le service."""
    NO_SERVICE              = "NO_SERVICE"
    REDUCED_SERVICE         = "REDUCED_SERVICE"
    SIGNIFICANT_DELAYS      = "SIGNIFICANT_DELAYS"
    DETOUR                  = "DETOUR"
    ADDITIONAL_SERVICE      = "ADDITIONAL_SERVICE"
    MODIFIED_SERVICE        = "MODIFIED_SERVICE"
    OTHER_EFFECT            = "OTHER_EFFECT"
    UNKNOWN_EFFECT          = "UNKNOWN_EFFECT"
    STOP_MOVED              = "STOP_MOVED"


# ---------------------------------------------------------------------------
# Stop Time Update — un arrêt dans un trip temps réel
# ---------------------------------------------------------------------------

class StopTimeEvent(BaseModel):
    """Heure de passage à un arrêt (arrivée ou départ)."""
    delay:  Optional[int]      = Field(None, description="Retard en secondes (négatif = avance)")
    time:   Optional[datetime] = Field(None, description="Heure absolue prévue")

    @field_validator("delay", mode="before")
    @classmethod
    def clamp_delay(cls, v):
        """Évite les valeurs aberrantes (retards > 24h probablement erronés)."""
        if v is not None and abs(v) > 86_400:
            return None
        return v


class StopTimeUpdate(BaseModel):
    """Mise à jour temps réel pour un arrêt donné d'un trip."""
    stop_sequence:         Optional[int]                    = None
    stop_id:               Optional[str]                    = None
    arrival:               Optional[StopTimeEvent]          = None
    departure:             Optional[StopTimeEvent]          = None
    schedule_relationship: StopScheduleRelationship         = StopScheduleRelationship.SCHEDULED

    @property
    def delay_seconds(self) -> Optional[int]:
        """Retard effectif : priorité départ, sinon arrivée."""
        if self.departure and self.departure.delay is not None:
            return self.departure.delay
        if self.arrival and self.arrival.delay is not None:
            return self.arrival.delay
        return None

    @property
    def delay_minutes(self) -> Optional[float]:
        if self.delay_seconds is not None:
            return round(self.delay_seconds / 60, 2)
        return None

    @property
    def is_delayed(self) -> bool:
        """True si le train est en retard de plus de 60 secondes."""
        return (self.delay_seconds or 0) > 60

    @property
    def is_cancelled(self) -> bool:
        return self.schedule_relationship == StopScheduleRelationship.SKIPPED


# ---------------------------------------------------------------------------
# Trip Descriptor — identifiant d'un voyage ferroviaire
# ---------------------------------------------------------------------------

class TripDescriptor(BaseModel):
    """Identifiant unique d'un trip SNCF."""
    trip_id:       Optional[str] = None
    route_id:      Optional[str] = None
    direction_id:  Optional[int] = None
    start_time:    Optional[str] = None   # Format HH:MM:SS
    start_date:    Optional[str] = None   # Format YYYYMMDD
    schedule_relationship: TripScheduleRelationship = TripScheduleRelationship.SCHEDULED

    @property
    def is_cancelled(self) -> bool:
        return self.schedule_relationship == TripScheduleRelationship.SKIPPED


# ---------------------------------------------------------------------------
# Trip Update — mise à jour complète d'un voyage
# ---------------------------------------------------------------------------

class TripUpdate(BaseModel):
    """
    Entité principale du flux GTFS-RT Trip Updates.
    Représente l'état temps réel d'un voyage ferroviaire complet.
    """
    entity_id:        str
    trip:             TripDescriptor
    stop_time_update: list[StopTimeUpdate]  = Field(default_factory=list)
    timestamp:        Optional[datetime]    = None

    # Métadonnées d'ingestion
    ingested_at:      datetime              = Field(default_factory=datetime.utcnow)
    source:           str                   = "gtfs-rt-sncf"

    @property
    def max_delay_seconds(self) -> Optional[int]:
        """Retard maximum observé sur tous les arrêts du trip."""
        delays = [
            s.delay_seconds for s in self.stop_time_update
            if s.delay_seconds is not None
        ]
        return max(delays) if delays else None

    @property
    def max_delay_minutes(self) -> Optional[float]:
        d = self.max_delay_seconds
        return round(d / 60, 2) if d is not None else None

    @property
    def has_cancellation(self) -> bool:
        """True si au moins un arrêt est annulé ou si le trip entier est supprimé."""
        return self.trip.is_cancelled or any(s.is_cancelled for s in self.stop_time_update)

    @property
    def affected_stops_count(self) -> int:
        """Nombre d'arrêts avec un retard > 60s."""
        return sum(1 for s in self.stop_time_update if s.is_delayed)


# ---------------------------------------------------------------------------
# Service Alert — perturbation sur le réseau
# ---------------------------------------------------------------------------

class TranslatedString(BaseModel):
    """Texte traduit (GTFS-RT supporte le multilingue)."""
    text:     str
    language: Optional[str] = "fr"


class AlertPeriod(BaseModel):
    """Période d'activité d'une alerte."""
    start: Optional[datetime] = None
    end:   Optional[datetime] = None


class ServiceAlert(BaseModel):
    """
    Perturbation ou alerte de service sur le réseau SNCF.
    Correspond aux entités du flux GTFS-RT Service Alerts.
    """
    entity_id:        str
    active_period:    list[AlertPeriod]       = Field(default_factory=list)
    cause:            AlertCause              = AlertCause.UNKNOWN_CAUSE
    effect:           AlertEffect             = AlertEffect.UNKNOWN_EFFECT
    header_text:      Optional[str]           = None
    description_text: Optional[str]           = None

    # Métadonnées d'ingestion
    ingested_at:      datetime                = Field(default_factory=datetime.utcnow)
    source:           str                     = "gtfs-rt-sncf-alerts"

    @property
    def is_active_now(self) -> bool:
        """Vérifie si l'alerte est active à l'instant présent."""
        now = datetime.utcnow()
        if not self.active_period:
            return True  # Pas de période = toujours active
        return any(
            (p.start is None or p.start <= now) and (p.end is None or p.end >= now)
            for p in self.active_period
        )

    @property
    def is_cancellation(self) -> bool:
        return self.effect == AlertEffect.NO_SERVICE


# ---------------------------------------------------------------------------
# Fetch Result — enveloppe d'un appel d'ingestion complet
# ---------------------------------------------------------------------------

class FetchResult(BaseModel):
    """
    Résultat d'un cycle d'ingestion complet.
    Sert de contrat de données entre le fetcher et le storage.
    Clé pour la gouvernance : traçabilité de chaque fetch.
    """
    fetched_at:      datetime           = Field(default_factory=datetime.utcnow)
    source_url:      str
    feed_timestamp:  Optional[datetime] = None   # Timestamp du flux GTFS-RT lui-même
    http_status:     int                = 200
    parse_duration_ms: Optional[float] = None

    # Données parsées
    trip_updates:    list[TripUpdate]   = Field(default_factory=list)
    service_alerts:  list[ServiceAlert] = Field(default_factory=list)

    # Métriques qualité — exposées dans le Data Quality Score
    entity_count:    int                = 0
    error_count:     int                = 0
    skipped_count:   int                = 0

    @property
    def success(self) -> bool:
        return self.http_status == 200 and self.error_count == 0

    @property
    def data_freshness_seconds(self) -> Optional[float]:
        """Âge de la donnée en secondes (vs timestamp du flux)."""
        if self.feed_timestamp:
            return (self.fetched_at - self.feed_timestamp).total_seconds()
        return None

    @property
    def quality_score(self) -> float:
        """
        Score de qualité brut entre 0 et 1.
        Formule : (entities_ok / total_entities) * freshness_bonus
        Utilisé dans le Data Quality Dashboard.
        """
        if self.entity_count == 0:
            return 0.0
        base = max(0.0, (self.entity_count - self.error_count) / self.entity_count)
        # Pénalité si la donnée a plus de 5 minutes de retard
        freshness = self.data_freshness_seconds
        if freshness and freshness > 300:
            base *= 0.8
        return round(base, 4)