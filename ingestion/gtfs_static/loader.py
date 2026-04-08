"""
GTFS Static Loader — Référentiel des gares SNCF
Charge stops.txt du GTFS statique dans bronze.stops (DuckDB).
Permet de résoudre code UIC → nom de gare.
Lance : python -m ingestion.gtfs_static.loader
"""

import asyncio
import io
import zipfile

import httpx
from loguru import logger

from storage.database import get_db

GTFS_STATIC_URL = "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip"


def _ensure_stops_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.stops (
            stop_id    VARCHAR PRIMARY KEY,
            stop_name  VARCHAR NOT NULL,
            stop_lat   DOUBLE,
            stop_lon   DOUBLE,
            uic_code   VARCHAR
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_stops_uic
            ON bronze.stops (uic_code)
    """)


async def load_gtfs_stops() -> int:
    """Télécharge le GTFS statique et charge stops.txt dans DuckDB."""
    logger.info("Téléchargement du GTFS statique SNCF...")

    async with httpx.AsyncClient(timeout=httpx.Timeout(300.0)) as client:
        response = await client.get(GTFS_STATIC_URL)
        response.raise_for_status()

    logger.info(f"GTFS statique téléchargé ({len(response.content) / 1024 / 1024:.1f} MB)")

    # Extrait uniquement stops.txt du ZIP
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        with z.open("stops.txt") as f:
            content = f.read().decode("utf-8")

    lines  = content.strip().split("\n")
    header = [h.strip() for h in lines[0].split(",")]

    # Trouve les index des colonnes
    idx = {col: i for i, col in enumerate(header)}
    id_i    = idx.get("stop_id", 0)
    name_i  = idx.get("stop_name", 1)
    lat_i   = idx.get("stop_lat")
    lon_i   = idx.get("stop_lon")

    inserted = 0
    with get_db() as conn:
        _ensure_stops_table(conn)
        # Vide et recharge (référentiel mis à jour quotidiennement)
        conn.execute("DELETE FROM bronze.stops")

        for line in lines[1:]:
            if not line.strip():
                continue
            parts = line.split(",")
            if len(parts) < 2:
                continue

            stop_id   = parts[id_i].strip().strip('"')
            stop_name = parts[name_i].strip().strip('"')

            # Latitude / longitude
            try:
                lat = float(parts[lat_i].strip()) if lat_i and lat_i < len(parts) else None
                lon = float(parts[lon_i].strip()) if lon_i and lon_i < len(parts) else None
            except (ValueError, TypeError):
                lat = lon = None

            # Extrait le code UIC depuis le stop_id
            # Format : "StopPoint:OCETrain TER-87576009" → UIC = "87576009"
            import re
            uic_match = re.search(r'(\d{8})', stop_id)
            uic_code  = uic_match.group(1) if uic_match else None

            try:
                conn.execute(
                    "INSERT OR IGNORE INTO bronze.stops VALUES (?, ?, ?, ?, ?)",
                    [stop_id, stop_name, lat, lon, uic_code]
                )
                inserted += 1
            except Exception:
                pass

    logger.success(f"Référentiel gares chargé : {inserted} arrêts")
    return inserted


def get_stop_name(uic_code: str) -> str | None:
    """Résout un code UIC en nom de gare. Utilisé par l'API."""
    try:
        with get_db(read_only=True) as conn:
            row = conn.execute(
                "SELECT stop_name FROM bronze.stops WHERE uic_code = ? LIMIT 1",
                [uic_code]
            ).fetchone()
            return row[0] if row else None
    except Exception:
        return None


if __name__ == "__main__":
    asyncio.run(load_gtfs_stops())