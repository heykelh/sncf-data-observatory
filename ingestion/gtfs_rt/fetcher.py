"""
GTFS-RT Fetcher
===============
Récupère les flux GTFS-RT SNCF (Trip Updates + Service Alerts) toutes les N secondes.
Utilise httpx async pour des performances maximales et APScheduler pour le scheduling.

Lancer directement :
    python -m ingestion.gtfs_rt.fetcher

Ou importer le scheduler dans FastAPI :
    from ingestion.gtfs_rt.fetcher import start_scheduler
"""

import asyncio
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.table import Table

from ingestion.gtfs_rt.models import FetchResult
from ingestion.gtfs_rt.parser import parse_feed

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

GTFS_RT_TRIP_UPDATES_URL: str = os.getenv(
    "GTFS_RT_TRIP_UPDATES_URL",
    "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates",
)

GTFS_RT_SERVICE_ALERTS_URL: str = os.getenv(
    "GTFS_RT_SERVICE_ALERTS_URL",
    "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-service-alerts",
)

FETCH_INTERVAL_SECONDS: int = int(os.getenv("FETCH_INTERVAL_SECONDS", "120"))

# Timeout httpx : 30s connexion, 60s lecture (flux protobuf peut être volumineux)
HTTP_TIMEOUT = httpx.Timeout(connect=30.0, read=60.0, write=10.0, pool=5.0)

# Headers pour identifier notre client auprès de transport.data.gouv.fr
HTTP_HEADERS = {
    "User-Agent": "SNCF-Data-Observatory/1.0 (github.com/heykelh/sncf-data-observatory)",
    "Accept": "application/x-protobuf, application/octet-stream",
}

console = Console()

# ---------------------------------------------------------------------------
# Logger setup : fichiers rotatifs + console colorée
# ---------------------------------------------------------------------------

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logger.remove()  # Supprime le handler par défaut
logger.add(
    sys.stderr,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="INFO",
    colorize=True,
)
logger.add(
    LOG_DIR / "fetcher_{time:YYYY-MM-DD}.log",
    rotation="00:00",      # Nouveau fichier chaque jour à minuit
    retention="7 days",    # Garde 7 jours de logs
    compression="zip",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
)


# ---------------------------------------------------------------------------
# Fetch d'un flux unique
# ---------------------------------------------------------------------------

async def fetch_single_feed(
    client:    httpx.AsyncClient,
    url:       str,
    feed_type: str,
) -> FetchResult:
    """
    Télécharge et parse un flux GTFS-RT depuis une URL donnée.

    Args:
        client:    Client HTTP httpx réutilisable (connexions persistantes)
        url:       URL du flux protobuf
        feed_type: "trip_updates" ou "service_alerts"

    Returns:
        FetchResult contenant les entités parsées + métriques qualité
    """
    logger.debug(f"Fetching {feed_type} depuis {url}")

    try:
        response = await client.get(url, headers=HTTP_HEADERS)
        response.raise_for_status()

        result = parse_feed(
            raw_bytes=response.content,
            source_url=url,
            http_status=response.status_code,
            feed_type=feed_type,
        )
        return result

    except httpx.TimeoutException:
        logger.error(f"Timeout sur {url} après {HTTP_TIMEOUT.read}s")
        return FetchResult(source_url=url, http_status=408, error_count=1)

    except httpx.HTTPStatusError as exc:
        logger.error(f"HTTP {exc.response.status_code} sur {url}")
        return FetchResult(source_url=url, http_status=exc.response.status_code, error_count=1)

    except Exception as exc:
        logger.exception(f"Erreur inattendue sur {url}: {exc}")
        return FetchResult(source_url=url, http_status=500, error_count=1)


# ---------------------------------------------------------------------------
# Cycle d'ingestion complet (les deux flux en parallèle)
# ---------------------------------------------------------------------------

async def fetch_all_feeds() -> tuple[FetchResult, FetchResult]:
    """
    Récupère Trip Updates ET Service Alerts en parallèle (asyncio.gather).
    Les deux appels HTTP partent simultanément — gain de temps ~50%.

    Returns:
        Tuple (trip_updates_result, service_alerts_result)
    """
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        trip_result, alert_result = await asyncio.gather(
            fetch_single_feed(client, GTFS_RT_TRIP_UPDATES_URL,   "trip_updates"),
            fetch_single_feed(client, GTFS_RT_SERVICE_ALERTS_URL, "service_alerts"),
        )

    return trip_result, alert_result


# ---------------------------------------------------------------------------
# Affichage console (Rich) — pour le développement et le monitoring
# ---------------------------------------------------------------------------

def _print_fetch_summary(trip_result: FetchResult, alert_result: FetchResult) -> None:
    """Affiche un résumé formaté dans la console après chaque cycle."""
    table = Table(
        title=f"[bold]Cycle d'ingestion — {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC[/bold]",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Flux",           style="dim",    width=20)
    table.add_column("Status",         justify="center", width=8)
    table.add_column("Entités",        justify="right",  width=9)
    table.add_column("Erreurs",        justify="right",  width=9)
    table.add_column("Parse (ms)",     justify="right",  width=11)
    table.add_column("Fraîcheur (s)",  justify="right",  width=13)
    table.add_column("Quality Score",  justify="right",  width=13)

    def _status_icon(r: FetchResult) -> str:
        return "[green]✓[/green]" if r.success else "[red]✗[/red]"

    for label, result in [("Trip Updates", trip_result), ("Service Alerts", alert_result)]:
        freshness = (
            f"{result.data_freshness_seconds:.0f}"
            if result.data_freshness_seconds is not None
            else "N/A"
        )
        quality_color = (
            "green" if result.quality_score >= 0.9
            else "yellow" if result.quality_score >= 0.7
            else "red"
        )
        table.add_row(
            label,
            _status_icon(result),
            str(result.entity_count),
            str(result.error_count),
            str(result.parse_duration_ms or "N/A"),
            freshness,
            f"[{quality_color}]{result.quality_score:.2%}[/{quality_color}]",
        )

    console.print(table)

    # Stats rapides sur les retards
    if trip_result.trip_updates:
        delayed = [t for t in trip_result.trip_updates if t.max_delay_minutes and t.max_delay_minutes > 1]
        cancelled = [t for t in trip_result.trip_updates if t.has_cancellation]
        console.print(
            f"  [yellow]Trains en retard :[/yellow] {len(delayed)} | "
            f"[red]Suppressions :[/red] {len(cancelled)} | "
            f"[cyan]Total trips :[/cyan] {len(trip_result.trip_updates)}"
        )

    if alert_result.service_alerts:
        active = [a for a in alert_result.service_alerts if a.is_active_now]
        console.print(f"  [red]Alertes actives :[/red] {len(active)} / {len(alert_result.service_alerts)}")

    console.print()


# ---------------------------------------------------------------------------
# Job schedulé — appelé par APScheduler toutes les N secondes
# ---------------------------------------------------------------------------

# Stockage en mémoire des derniers résultats (partagé avec FastAPI plus tard)
_last_trip_result:  FetchResult | None = None
_last_alert_result: FetchResult | None = None

# Compteur de cycles pour le monitoring
_cycle_count: int = 0


async def _scheduled_fetch_job() -> None:
    """
    Job principal exécuté par APScheduler.
    Fetch → Parse → (futur: Storage) → Log.
    """
    global _last_trip_result, _last_alert_result, _cycle_count
    _cycle_count += 1

    logger.info(f"=== Cycle #{_cycle_count} démarré ===")

    trip_result, alert_result = await fetch_all_feeds()

    # Mise à jour du cache mémoire (FastAPI pourra lire ces données)
    _last_trip_result  = trip_result
    _last_alert_result = alert_result

    # Affichage console
    _print_fetch_summary(trip_result, alert_result)

    # écriture en DuckDB via storage.writer
    from storage.writer import write_fetch_results
    write_stats = write_fetch_results(trip_result, alert_result)
    logger.info(f"Écriture DB terminée: {write_stats}")

    logger.info(f"=== Cycle #{_cycle_count} terminé ===\n")


# ---------------------------------------------------------------------------
# Accesseurs pour FastAPI (couche API — semaine 4)
# ---------------------------------------------------------------------------

def get_last_trip_result() -> FetchResult | None:
    """Retourne le dernier FetchResult Trip Updates (pour FastAPI)."""
    return _last_trip_result


def get_last_alert_result() -> FetchResult | None:
    """Retourne le dernier FetchResult Service Alerts (pour FastAPI)."""
    return _last_alert_result


def get_cycle_count() -> int:
    return _cycle_count


# ---------------------------------------------------------------------------
# Scheduler APScheduler
# ---------------------------------------------------------------------------

_scheduler: AsyncIOScheduler | None = None


def start_scheduler() -> AsyncIOScheduler:
    """
    Démarre le scheduler APScheduler.
    Appelable depuis FastAPI (lifespan) ou standalone.

    Returns:
        L'instance du scheduler (pour pouvoir l'arrêter proprement).
    """
    global _scheduler
    _scheduler = AsyncIOScheduler(timezone="UTC")
    _scheduler.add_job(
        _scheduled_fetch_job,
        trigger="interval",
        seconds=FETCH_INTERVAL_SECONDS,
        id="gtfs_rt_fetch",
        name="SNCF GTFS-RT Ingestion",
        replace_existing=True,
        max_instances=1,           # Évite les chevauchements si un cycle est lent
        misfire_grace_time=30,     # Tolère 30s de retard avant d'annuler le job
    )
    _scheduler.start()
    logger.info(
        f"Scheduler démarré — intervalle : {FETCH_INTERVAL_SECONDS}s "
        f"({FETCH_INTERVAL_SECONDS // 60}min {FETCH_INTERVAL_SECONDS % 60}s)"
    )
    return _scheduler


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler arrêté proprement.")


# ---------------------------------------------------------------------------
# Point d'entrée standalone : python -m ingestion.gtfs_rt.fetcher
# ---------------------------------------------------------------------------

async def _main() -> None:
    """
    Lance le fetcher en mode standalone avec un premier fetch immédiat.
    Ctrl+C pour arrêter proprement.
    """
    console.rule("[bold cyan]SNCF Data Observatory — GTFS-RT Fetcher[/bold cyan]")
    console.print(f"  Trip Updates URL   : [dim]{GTFS_RT_TRIP_UPDATES_URL}[/dim]")
    console.print(f"  Service Alerts URL : [dim]{GTFS_RT_SERVICE_ALERTS_URL}[/dim]")
    console.print(f"  Intervalle         : [bold]{FETCH_INTERVAL_SECONDS}s[/bold]")
    console.print()

    # Arrêt propre sur Ctrl+C / SIGTERM
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_scheduler)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: stop_scheduler())

    # Premier fetch immédiat sans attendre le premier intervalle
    console.print("[yellow]Premier fetch immédiat...[/yellow]")
    await _scheduled_fetch_job()

    # Démarrage du scheduler pour les cycles suivants
    start_scheduler()

    console.print(f"[green]Pipeline actif. Prochain fetch dans {FETCH_INTERVAL_SECONDS}s.[/green]")
    console.print("[dim]Ctrl+C pour arrêter proprement.[/dim]\n")

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        stop_scheduler()
        console.print("\n[yellow]Arrêt du fetcher.[/yellow]")

if __name__ == "__main__":
    asyncio.run(_main())