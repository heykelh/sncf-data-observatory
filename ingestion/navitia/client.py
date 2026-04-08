"""
Navitia API Client
==================
Enrichit les données GTFS-RT brutes avec des informations métier lisibles :
  - Nom commercial du train (ex: "TGV inOUI 6201")
  - Noms des gares (ex: "Paris Montparnasse" au lieu de "StopPoint:OCE87391003")
  - Prochains départs en temps réel depuis une gare
  - Informations de perturbation liées à un trip

Quota : 5 000 requêtes/jour (plan gratuit).
Stratégie : on n'appelle Navitia que pour enrichir, jamais à chaque cycle GTFS-RT.
Le cache Redis (ou dict mémoire en dev) limite les appels redondants.

Base URL : https://api.sncf.com/v1/coverage/sncf/
Auth     : Basic HTTP — token en username, password vide.

Lancer en standalone pour tester :
    python -m ingestion.navitia.client
"""

import base64
import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from dotenv import load_dotenv
from loguru import logger
from pydantic import BaseModel, Field

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NAVITIA_API_KEY: str = os.getenv("NAVITIA_API_KEY", "")
NAVITIA_BASE_URL: str = "https://api.sncf.com/v1/coverage/sncf"
NAVITIA_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=5.0, pool=5.0)

# Quota journalier : 5 000 req/jour = ~3.47 req/min
# On se limite à 3 req/min pour garder de la marge
NAVITIA_MIN_INTERVAL_S: float = 0.35  # ~170ms entre chaque requête

# Cache mémoire simple (trip_id → enrichissement)
# En production : remplacer par Redis via storage/cache.py
_STOP_CACHE:  dict[str, dict] = {}   # stop_id → stop info
_TRIP_CACHE:  dict[str, dict] = {}   # trip_id → trip info
_CACHE_TTL_S: int = 3600             # 1h — les infos de gares ne changent pas


# ---------------------------------------------------------------------------
# Modèles Pydantic — sortie du client Navitia
# ---------------------------------------------------------------------------

class StopInfo(BaseModel):
    """Informations enrichies sur une gare."""
    stop_id:    str
    name:       str
    label:      Optional[str] = None          # Nom complet (ex: "Paris Montparnasse")
    coord_lat:  Optional[float] = None
    coord_lon:  Optional[float] = None
    uic_code:   Optional[str] = None          # Code UIC SNCF


class Disruption(BaseModel):
    """Perturbation Navitia associée à un trip ou une gare."""
    disruption_id: str
    status:        str                        # "active" | "future" | "past"
    severity:      str                        # "NO_SERVICE" | "SIGNIFICANT_DELAYS" | etc.
    cause:         Optional[str] = None
    message:       Optional[str] = None
    start:         Optional[datetime] = None
    end:           Optional[datetime] = None


class TripInfo(BaseModel):
    """Informations enrichies sur un voyage ferroviaire."""
    trip_id:         str
    headsign:        Optional[str] = None     # Numéro commercial (ex: "6201")
    name:            Optional[str] = None     # Nom lisible (ex: "TGV inOUI 6201")
    direction:       Optional[str] = None     # Gare terminus
    physical_mode:   Optional[str] = None     # "LongDistanceTrain" | "LocalTrain" etc.
    commercial_mode: Optional[str] = None     # "TGV" | "TER" | "Intercités" etc.
    disruptions:     list[Disruption] = Field(default_factory=list)
    fetched_at:      datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Departure(BaseModel):
    """Prochain départ depuis une gare (temps réel si disponible)."""
    stop_name:       str
    route_name:      str                      # Nom de la ligne
    direction:       str                      # Gare terminus
    headsign:        Optional[str] = None     # Numéro du train
    commercial_mode: Optional[str] = None
    base_departure:  Optional[datetime] = None   # Horaire théorique
    rt_departure:    Optional[datetime] = None   # Horaire temps réel
    delay_minutes:   Optional[float] = None
    status:          str = "scheduled"        # "scheduled" | "delayed" | "cancelled"
    disruptions:     list[Disruption] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Client HTTP Navitia
# ---------------------------------------------------------------------------

class NavitiaClient:
    """
    Client async pour l'API SNCF/Navitia.

    Usage :
        async with NavitiaClient() as client:
            trip = await client.get_trip_info("OCESN64003R")
            departures = await client.get_departures("stop_area:OCE87686006")
    """

    def __init__(self, api_key: str = NAVITIA_API_KEY):
        if not api_key:
            logger.warning(
                "NAVITIA_API_KEY non définie — le client fonctionnera "
                "en mode dégradé (pas d'enrichissement). "
                "Inscris-toi sur https://numerique.sncf.com/startup/api/"
            )
        self._api_key = api_key
        self._last_request_time: float = 0.0
        self._client: Optional[httpx.AsyncClient] = None

    # ── Gestion du context manager async ──────────────────────────────────

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            base_url=NAVITIA_BASE_URL,
            timeout=NAVITIA_TIMEOUT,
            headers=self._build_headers(),
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    # ── Auth ──────────────────────────────────────────────────────────────

    def _build_headers(self) -> dict:
        """
        Navitia utilise Basic HTTP Auth : token en username, pas de password.
        Format : Authorization: Basic base64(token:)
        """
        if not self._api_key:
            return {}
        credentials = base64.b64encode(f"{self._api_key}:".encode()).decode()
        return {
            "Authorization": f"Basic {credentials}",
            "User-Agent": "SNCF-Data-Observatory/1.0 (github.com/heykelh/sncf-data-observatory)",
        }

    # ── Rate limiting ──────────────────────────────────────────────────────

    async def _rate_limit(self) -> None:
        """Respecte le délai minimum entre deux requêtes."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < NAVITIA_MIN_INTERVAL_S:
            import asyncio
            await asyncio.sleep(NAVITIA_MIN_INTERVAL_S - elapsed)
        self._last_request_time = time.monotonic()

    # ── Requête HTTP générique ─────────────────────────────────────────────

    async def _get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """
        Effectue une requête GET sur l'API Navitia.
        Retourne le JSON parsé ou None en cas d'erreur.
        """
        if not self._api_key:
            return None

        await self._rate_limit()

        try:
            response = await self._client.get(endpoint, params=params or {})

            if response.status_code == 401:
                logger.error("Navitia : clé API invalide ou expirée")
                return None
            if response.status_code == 429:
                logger.warning("Navitia : quota journalier atteint (5 000 req/jour)")
                return None
            if response.status_code == 404:
                logger.debug(f"Navitia 404 : {endpoint}")
                return None

            response.raise_for_status()
            return response.json()

        except httpx.TimeoutException:
            logger.warning(f"Navitia timeout sur {endpoint}")
            return None
        except Exception as exc:
            logger.warning(f"Navitia erreur sur {endpoint}: {exc}")
            return None

    # ── Parsing des disruptions ────────────────────────────────────────────

    def _parse_disruptions(self, raw_disruptions: list) -> list[Disruption]:
        """Parse les disruptions Navitia en modèles Pydantic."""
        disruptions = []
        for d in raw_disruptions or []:
            try:
                # Récupère le premier message disponible
                messages = d.get("messages", [])
                message_text = messages[0].get("text") if messages else None

                # Périodes d'activité
                periods = d.get("application_periods", [])
                start = end = None
                if periods:
                    raw_start = periods[0].get("begin")
                    raw_end   = periods[0].get("end")
                    if raw_start:
                        start = datetime.strptime(raw_start, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
                    if raw_end:
                        end = datetime.strptime(raw_end, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)

                disruptions.append(Disruption(
                    disruption_id=d.get("disruption_id", d.get("id", "unknown")),
                    status=d.get("status", "unknown"),
                    severity=d.get("severity", {}).get("effect", "UNKNOWN_EFFECT"),
                    cause=d.get("cause"),
                    message=message_text,
                    start=start,
                    end=end,
                ))
            except Exception as exc:
                logger.debug(f"Disruption ignorée : {exc}")
        return disruptions

    # ── API publique ───────────────────────────────────────────────────────

    async def get_stop_info(self, stop_id: str) -> Optional[StopInfo]:
        """
        Récupère les informations d'une gare à partir de son stop_id GTFS-RT.

        Le stop_id du GTFS-RT ressemble à "StopPoint:OCE87391003".
        On extrait le code UIC (87391003) pour interroger Navitia.

        Args:
            stop_id: Identifiant de gare issu du flux GTFS-RT

        Returns:
            StopInfo enrichi ou None si introuvable
        """
        if stop_id in _STOP_CACHE:
            cached = _STOP_CACHE[stop_id]
            if time.monotonic() - cached["_cached_at"] < _CACHE_TTL_S:
                return StopInfo(**{k: v for k, v in cached.items() if k != "_cached_at"})

        # Navitia attend le format "stop_area:OCE87391003" ou "stop_point:..."
        navitia_id = stop_id.replace("StopPoint:", "stop_point:").replace("StopArea:", "stop_area:")

        data = await self._get(f"/stop_points/{navitia_id}")
        if not data:
            # Fallback : cherche par code UIC si le stop_id contient un code numérique
            uic = "".join(filter(str.isdigit, stop_id))
            if uic:
                data = await self._get("/stop_points", params={"filter": f"stop_point.codes.type=uic;stop_point.codes.value={uic}"})

        if not data:
            return None

        # Navitia retourne parfois une liste, parfois un objet direct
        stop_points = data.get("stop_points", [data.get("stop_point", {})])
        if not stop_points:
            return None

        sp = stop_points[0]
        coord = sp.get("coord", {})

        # Cherche le code UIC dans les codes
        uic_code = None
        for code in sp.get("codes", []):
            if code.get("type") == "uic":
                uic_code = code.get("value")
                break

        info = StopInfo(
            stop_id=stop_id,
            name=sp.get("name", stop_id),
            label=sp.get("label"),
            coord_lat=float(coord["lat"]) if coord.get("lat") else None,
            coord_lon=float(coord["lon"]) if coord.get("lon") else None,
            uic_code=uic_code,
        )

        # Mise en cache
        _STOP_CACHE[stop_id] = {**info.model_dump(), "_cached_at": time.monotonic()}
        return info

    async def get_trip_info(self, trip_id: str) -> Optional[TripInfo]:
        """
        Récupère les informations commerciales d'un trip GTFS-RT.

        Le trip_id GTFS-RT ressemble à "OCESN64003R" ou
        "OCESN44212R1187_R:CTE:FR:Line::...". On essaie d'abord
        le trip_id direct, puis on extrait le numéro commercial.

        Args:
            trip_id: Identifiant de voyage issu du GTFS-RT

        Returns:
            TripInfo enrichi avec nom commercial, mode, disruptions
        """
        if trip_id in _TRIP_CACHE:
            cached = _TRIP_CACHE[trip_id]
            if time.monotonic() - cached["_cached_at"] < _CACHE_TTL_S:
                return TripInfo(**{k: v for k, v in cached.items() if k != "_cached_at"})

        # Essai 1 : trip_id direct en vehicle_journey
        data = await self._get(f"/vehicle_journeys/{trip_id}")

        # Essai 2 : chercher par headsign (numéro commercial extrait du trip_id)
        if not data:
            # Extrait le numéro de train : ex "OCESN64003R" → "64003"
            import re
            match = re.search(r'SN(\d{4,6})', trip_id)
            if match:
                train_num = match.group(1)
                data = await self._get(
                    "/vehicle_journeys",
                    params={"headsign": train_num, "count": 1}
                )

        if not data:
            return None

        # Parse le premier vehicle_journey trouvé
        vjs = data.get("vehicle_journeys", [data.get("vehicle_journey")])
        if not vjs or not vjs[0]:
            return None

        vj = vjs[0]
        disruptions = self._parse_disruptions(data.get("disruptions", []))

        # Extraction du mode commercial
        commercial_mode = None
        physical_mode   = None
        journey_pattern = vj.get("journey_pattern", {})
        route = journey_pattern.get("route", {})
        line  = route.get("line", {})

        if line.get("commercial_mode"):
            commercial_mode = line["commercial_mode"].get("name")
        if vj.get("physical_mode"):
            physical_mode = vj["physical_mode"].get("name")

        # Direction = terminus du trajet
        direction = None
        stop_times = vj.get("stop_times", [])
        if stop_times:
            last_stop = stop_times[-1]
            stop_point = last_stop.get("stop_point", {})
            direction = stop_point.get("name") or stop_point.get("label")

        info = TripInfo(
            trip_id=trip_id,
            headsign=vj.get("headsign"),
            name=vj.get("name") or vj.get("headsign"),
            direction=direction,
            physical_mode=physical_mode,
            commercial_mode=commercial_mode,
            disruptions=disruptions,
        )

        _TRIP_CACHE[trip_id] = {**info.model_dump(), "_cached_at": time.monotonic()}
        return info

    async def get_departures(
        self,
        stop_id:  str,
        count:    int = 10,
        duration: int = 3600,
    ) -> list[Departure]:
        """
        Récupère les prochains départs depuis une gare en temps réel.

        Utilisé pour enrichir le dashboard : afficher les prochains trains
        d'une gare sélectionnée sur la carte avec leurs retards.

        Args:
            stop_id:  stop_area ou stop_point ID (format GTFS-RT ou Navitia)
            count:    Nombre maximum de départs à retourner
            duration: Fenêtre de temps en secondes (défaut : 1h)

        Returns:
            Liste de Departure avec retards temps réel si disponibles
        """
        navitia_id = stop_id.replace("StopPoint:", "stop_point:").replace("StopArea:", "stop_area:")

        # Utilise l'endpoint departures qui intègre le temps réel
        data = await self._get(
            f"/stop_areas/{navitia_id}/departures",
            params={
                "count":    count,
                "duration": duration,
                "data_freshness": "realtime",   # Demande explicitement le temps réel
            }
        )

        if not data:
            return []

        departures = []
        all_disruptions = self._parse_disruptions(data.get("disruptions", []))

        for raw in data.get("departures", []):
            try:
                route = raw.get("route", {})
                display = raw.get("display_informations", {})
                stop_dt = raw.get("stop_date_time", {})

                # Horaires théorique vs temps réel
                base_dt = stop_dt.get("base_departure_date_time")
                rt_dt   = stop_dt.get("departure_date_time")

                def _parse_dt(s: str) -> Optional[datetime]:
                    if not s:
                        return None
                    try:
                        return datetime.strptime(s, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
                    except Exception:
                        return None

                base_departure = _parse_dt(base_dt)
                rt_departure   = _parse_dt(rt_dt)

                # Calcul du retard
                delay_minutes = None
                if base_departure and rt_departure:
                    delta = (rt_departure - base_departure).total_seconds() / 60
                    delay_minutes = round(delta, 1)

                # Statut
                data_freshness = stop_dt.get("data_freshness", "scheduled")
                if stop_dt.get("departure_date_time") is None:
                    status = "cancelled"
                elif delay_minutes and delay_minutes > 1:
                    status = "delayed"
                else:
                    status = "scheduled"

                departures.append(Departure(
                    stop_name=display.get("label", stop_id),
                    route_name=display.get("label", route.get("name", "")),
                    direction=display.get("direction", ""),
                    headsign=display.get("headsign"),
                    commercial_mode=display.get("commercial_mode"),
                    base_departure=base_departure,
                    rt_departure=rt_departure,
                    delay_minutes=delay_minutes,
                    status=status,
                    disruptions=[
                        d for d in all_disruptions
                        if d.disruption_id in [
                            rd.get("id", "") for rd in raw.get("display_informations", {}).get("disruptions", [])
                        ]
                    ],
                ))
            except Exception as exc:
                logger.debug(f"Départ ignoré : {exc}")

        logger.info(f"Navitia → {len(departures)} départs depuis {stop_id}")
        return departures

    async def get_disruptions_on_line(self, line_id: str) -> list[Disruption]:
        """
        Récupère toutes les perturbations actives sur une ligne.
        Utile pour croiser avec les Service Alerts GTFS-RT.

        Args:
            line_id: Identifiant de ligne Navitia (ex: "line:OCE:C00002")
        """
        data = await self._get(
            f"/lines/{line_id}/disruptions",
            params={"current_datetime": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")}
        )
        if not data:
            return []
        return self._parse_disruptions(data.get("disruptions", []))

    async def search_stop_by_name(self, name: str, count: int = 5) -> list[StopInfo]:
        """
        Recherche une gare par son nom en langage naturel.
        Utile pour le dashboard (barre de recherche).

        Args:
            name:  Nom de la gare (ex: "Paris Montparnasse", "Lyon Part-Dieu")
            count: Nombre max de résultats
        """
        data = await self._get(
            "/places",
            params={
                "q":     name,
                "type[]": "stop_area",
                "count": count,
            }
        )
        if not data:
            return []

        results = []
        for place in data.get("places", []):
            if place.get("embedded_type") != "stop_area":
                continue
            sa = place.get("stop_area", {})
            coord = sa.get("coord", {})
            results.append(StopInfo(
                stop_id=sa.get("id", ""),
                name=sa.get("name", name),
                label=place.get("name"),
                coord_lat=float(coord["lat"]) if coord.get("lat") else None,
                coord_lon=float(coord["lon"]) if coord.get("lon") else None,
            ))

        return results


# ---------------------------------------------------------------------------
# Enrichisseur de batch — utilisé par le pipeline après le fetch GTFS-RT
# ---------------------------------------------------------------------------

async def enrich_trip_updates(trip_ids: list[str], max_enrichments: int = 20) -> dict[str, TripInfo]:
    """
    Enrichit un batch de trip_ids avec les infos Navitia.

    On ne fait PAS un appel Navitia par trip à chaque cycle GTFS-RT
    (ça épuiserait le quota de 5 000 req/jour en 30 minutes).

    Stratégie :
    - On enrichit seulement les trip_ids pas encore en cache
    - On limite à max_enrichments par cycle
    - Le cache TTL de 1h couvre ~30 cycles sans re-appel

    Args:
        trip_ids:        Liste de trip_ids issus du GTFS-RT
        max_enrichments: Nombre max d'appels Navitia par appel de fonction

    Returns:
        Dict trip_id → TripInfo pour les trips enrichis
    """
    if not NAVITIA_API_KEY:
        logger.debug("NAVITIA_API_KEY absent — enrichissement ignoré")
        return {}

    # Filtre : seulement les trips pas en cache
    uncached = [tid for tid in trip_ids if tid not in _TRIP_CACHE][:max_enrichments]

    if not uncached:
        logger.debug(f"Tous les trips en cache — 0 appel Navitia nécessaire")
        return {tid: TripInfo(**{k: v for k, v in _TRIP_CACHE[tid].items() if k != "_cached_at"})
                for tid in trip_ids if tid in _TRIP_CACHE}

    results = {}
    async with NavitiaClient() as client:
        for trip_id in uncached:
            info = await client.get_trip_info(trip_id)
            if info:
                results[trip_id] = info
                logger.debug(f"Enrichi : {trip_id} → {info.name or info.headsign} ({info.commercial_mode})")

    logger.info(f"Navitia enrichissement : {len(results)}/{len(uncached)} trips enrichis")
    return results


# ---------------------------------------------------------------------------
# Point d'entrée standalone : python -m ingestion.navitia.client
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import asyncio
    from rich.console import Console
    from rich.table import Table

    console = Console()

    async def _demo():
        console.rule("[bold cyan]SNCF Data Observatory — Navitia Client Demo[/bold cyan]")

        if not NAVITIA_API_KEY:
            console.print("[red]NAVITIA_API_KEY non définie dans .env[/red]")
            console.print("Inscris-toi sur : [link]https://numerique.sncf.com/startup/api/[/link]")
            return

        async with NavitiaClient() as client:

            # Test 1 : recherche de gare
            console.print("\n[bold]Test 1 — Recherche gare 'Paris Montparnasse'[/bold]")
            stops = await client.search_stop_by_name("Paris Montparnasse", count=3)
            for s in stops:
                console.print(f"  → {s.name} | id={s.stop_id} | lat={s.coord_lat}, lon={s.coord_lon}")

            # Test 2 : prochains départs depuis Paris Montparnasse
            if stops:
                console.print(f"\n[bold]Test 2 — Prochains départs depuis {stops[0].name}[/bold]")
                departures = await client.get_departures(stops[0].stop_id, count=5)

                table = Table(show_header=True, header_style="bold cyan")
                table.add_column("Train",     width=12)
                table.add_column("Direction", width=25)
                table.add_column("Mode",      width=12)
                table.add_column("Départ",    width=10)
                table.add_column("Retard",    width=10)
                table.add_column("Statut",    width=12)

                for d in departures:
                    rt_str = d.rt_departure.strftime("%H:%M") if d.rt_departure else "N/A"
                    delay_str = f"+{d.delay_minutes:.0f} min" if d.delay_minutes and d.delay_minutes > 0 else "À l'heure"
                    status_color = "red" if d.status == "cancelled" else "yellow" if d.status == "delayed" else "green"
                    table.add_row(
                        d.headsign or "—",
                        d.direction[:24] if d.direction else "—",
                        d.commercial_mode or "—",
                        rt_str,
                        f"[{status_color}]{delay_str}[/{status_color}]",
                        f"[{status_color}]{d.status}[/{status_color}]",
                    )

                console.print(table)

            # Test 3 : enrichissement d'un trip_id fictif GTFS-RT
            console.print("\n[bold]Test 3 — Enrichissement trip_id GTFS-RT[/bold]")
            # trip_id typique du flux GTFS-RT SNCF
            test_trip_id = "OCESN64003R"
            info = await client.get_trip_info(test_trip_id)
            if info:
                console.print(f"  trip_id   : {info.trip_id}")
                console.print(f"  Nom       : {info.name}")
                console.print(f"  Headsign  : {info.headsign}")
                console.print(f"  Mode      : {info.commercial_mode} / {info.physical_mode}")
                console.print(f"  Direction : {info.direction}")
                console.print(f"  Perturb.  : {len(info.disruptions)} disruption(s)")
            else:
                console.print(f"  [yellow]Trip {test_trip_id} non trouvé (normal si plus en circulation)[/yellow]")

        console.print("\n[green]Demo terminée.[/green]")

    asyncio.run(_demo())