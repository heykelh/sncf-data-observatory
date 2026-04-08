"""
Regularity Loader
=================
Charge les datasets historiques de régularité mensuelle SNCF depuis l'API
Open Data de ressources.data.sncf.com.

4 sources distinctes :
  - TGV      : régularité mensuelle nationale depuis 2013
  - TER      : régularité mensuelle par région depuis 2013
  - Transilien: ponctualité mensuelle depuis 2013
  - Intercités: régularité mensuelle depuis 2014

Ces données alimentent :
  - La couche Bronze (bronze.regularity_raw)
  - Les graphiques historiques du dashboard
  - Le contexte de comparaison pour les agents LangChain (V2)

Fréquence : 1 chargement par jour (cron à minuit) — données mensuelles.

Lancer en standalone :
    python -m ingestion.regularity.loader
"""

import asyncio
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import httpx
from dotenv import load_dotenv
from loguru import logger
from pydantic import BaseModel, Field, field_validator

from storage.database import get_db

load_dotenv()

# ---------------------------------------------------------------------------
# Config — URLs API ODS (OpenDataSoft) de ressources.data.sncf.com
# ---------------------------------------------------------------------------

REGULARITY_TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=5.0, pool=5.0)

REGULARITY_SOURCES = {
    "tgv": {
        "name":    "Régularité mensuelle TGV",
        "url":     "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/reglarite-mensuelle-tgv-nationale/exports/json",
        "params":  {"limit": -1, "timezone": "UTC"},
        "type":    "tgv",
    },
    "tgv_axe": {
        "name":    "Régularité mensuelle TGV par axe",
        "url":     "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/regularite-mensuelle-tgv-axes/exports/json",
        "params":  {"limit": -1, "timezone": "UTC"},
        "type":    "tgv_axe",
    },
    "ter": {
        "name":    "Régularité mensuelle TER",
        "url":     "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/regularite-mensuelle-ter/exports/json",
        "params":  {"limit": -1, "timezone": "UTC"},
        "type":    "ter",
    },
    "transilien": {
        "name":    "Ponctualité mensuelle Transilien",
        "url":     "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/ponctualite-mensuelle-transilien/exports/json",
        "params":  {"limit": -1, "timezone": "UTC"},
        "type":    "transilien",
    },
    "intercites": {
        "name":    "Régularité mensuelle Intercités",
        "url":     "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/regularite-mensuelle-intercites/exports/json",
        "params":  {"limit": -1, "timezone": "UTC"},
        "type":    "intercites",
    },
}


# ---------------------------------------------------------------------------
# Modèles Pydantic — représentation unifiée d'un enregistrement de régularité
# ---------------------------------------------------------------------------

class TrainType(str, Enum):
    TGV        = "tgv"
    TGV_AXE    = "tgv_axe"
    TER        = "ter"
    TRANSILIEN = "transilien"
    INTERCITES = "intercites"


class RegularityRecord(BaseModel):
    """
    Enregistrement unifié de régularité mensuelle.
    Normalise les différents formats des 4 sources en un schéma commun.
    """
    # Clé temporelle
    year:          int
    month:         int                        # 1–12
    period_label:  str                        # ex: "2024-03"

    # Segmentation
    train_type:    TrainType
    axe:           Optional[str] = None       # TGV : Paris-Lyon, etc.
    region:        Optional[str] = None       # TER : Normandie, etc.
    ligne:         Optional[str] = None       # Transilien : RER A, etc.

    # Métriques de ponctualité
    punctuality_rate:     Optional[float] = None  # Taux de régularité (0–100)
    trains_programmed:    Optional[int]   = None  # Trains programmés
    trains_cancelled:     Optional[int]   = None  # Trains annulés
    trains_late:          Optional[int]   = None  # Trains en retard

    # Commentaires SNCF
    comment:       Optional[str] = None

    # Métadonnées
    source_id:     str                        # "tgv" | "ter" | etc.
    loaded_at:     datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @field_validator("punctuality_rate", mode="before")
    @classmethod
    def parse_rate(cls, v):
        """Normalise le taux : accepte float, str avec virgule, ou None."""
        if v is None:
            return None
        if isinstance(v, str):
            v = v.replace(",", ".").strip().rstrip("%")
            if not v:
                return None
        try:
            rate = float(v)
            # Certains datasets expriment en 0–1, d'autres en 0–100
            if 0 < rate <= 1:
                rate *= 100
            return round(rate, 2)
        except (ValueError, TypeError):
            return None

    @field_validator("trains_programmed", "trains_cancelled", "trains_late", mode="before")
    @classmethod
    def parse_int(cls, v):
        if v is None:
            return None
        try:
            return int(str(v).replace(" ", "").replace(",", ""))
        except (ValueError, TypeError):
            return None

    @property
    def on_time_rate(self) -> Optional[float]:
        return self.punctuality_rate

    @property
    def delay_rate(self) -> Optional[float]:
        if self.punctuality_rate is not None:
            return round(100 - self.punctuality_rate, 2)
        return None


# ---------------------------------------------------------------------------
# Parsers par source — chaque API a son propre format de champs
# ---------------------------------------------------------------------------

def _parse_period(raw: str) -> tuple[int, int, str]:
    """
    Extrait année et mois depuis les formats variés des datasets.
    Formats observés : "2024-03", "2024-03-01", "03/2024", "2024 - mars"
    Retourne (année, mois, label)
    """
    import re

    raw = str(raw).strip()

    # Format ISO : "2024-03" ou "2024-03-01"
    m = re.match(r"(\d{4})-(\d{2})", raw)
    if m:
        return int(m.group(1)), int(m.group(2)), f"{m.group(1)}-{m.group(2)}"

    # Format "03/2024"
    m = re.match(r"(\d{2})/(\d{4})", raw)
    if m:
        return int(m.group(2)), int(m.group(1)), f"{m.group(2)}-{m.group(1)}"

    # Format "2024 - mars" (noms de mois FR)
    mois_fr = {
        "janvier": 1, "février": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8,
        "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12
    }
    m = re.match(r"(\d{4})\s*[-–]\s*(\w+)", raw, re.IGNORECASE)
    if m:
        year = int(m.group(1))
        month_name = m.group(2).lower()
        month = mois_fr.get(month_name, 1)
        return year, month, f"{year}-{month:02d}"

    # Fallback : essaie de parser une date ISO complète
    try:
        dt = datetime.fromisoformat(raw[:10])
        return dt.year, dt.month, f"{dt.year}-{dt.month:02d}"
    except Exception:
        pass

    logger.warning(f"Période non parsée : '{raw}' — valeur par défaut 2000-01")
    return 2000, 1, "2000-01"


def _parse_tgv(raw: dict, source_id: str) -> Optional[RegularityRecord]:
    """Parse un enregistrement du dataset TGV national."""
    try:
        period_raw = raw.get("date") or raw.get("periode") or raw.get("annee_mois", "")
        year, month, label = _parse_period(str(period_raw))

        return RegularityRecord(
            year=year,
            month=month,
            period_label=label,
            train_type=TrainType.TGV,
            punctuality_rate=(
            raw.get("taux_de_regularite") 
            or raw.get("regularite")
            or raw.get("ponctualite_origine")
            or raw.get("regularite_composite")
            ),
            trains_programmed=raw.get("nombre_de_trains_programmes") or raw.get("trains_programmes"),
            trains_cancelled=raw.get("nombre_de_trains_annules") or raw.get("trains_annules"),
            trains_late=raw.get("nombre_de_trains_en_retard_a_l_arrivee") or raw.get("trains_retard"),
            comment=raw.get("commentaires") or raw.get("commentaire"),
            source_id=source_id,
        )
    except Exception as exc:
        logger.debug(f"Ligne TGV ignorée : {exc} | raw={raw}")
        return None


def _parse_tgv_axe(raw: dict, source_id: str) -> Optional[RegularityRecord]:
    """Parse un enregistrement du dataset TGV par axe."""
    try:
        period_raw = raw.get("date") or raw.get("periode") or raw.get("annee_mois", "")
        year, month, label = _parse_period(str(period_raw))

        return RegularityRecord(
            year=year,
            month=month,
            period_label=label,
            train_type=TrainType.TGV_AXE,
            axe=raw.get("axe") or raw.get("liaison"),
            punctuality_rate=(
            raw.get("taux_de_regularite") 
            or raw.get("regularite")
            or raw.get("ponctualite_origine")
            or raw.get("regularite_composite")
            ),
            trains_programmed=raw.get("nombre_de_trains_programmes"),
            trains_cancelled=raw.get("nombre_de_trains_annules"),
            trains_late=raw.get("nombre_de_trains_en_retard_a_l_arrivee"),
            comment=raw.get("commentaires"),
            source_id=source_id,
        )
    except Exception as exc:
        logger.debug(f"Ligne TGV axe ignorée : {exc}")
        return None


def _parse_ter(raw: dict, source_id: str) -> Optional[RegularityRecord]:
    """Parse un enregistrement du dataset TER."""
    try:
        period_raw = raw.get("date") or raw.get("periode") or raw.get("annee_mois", "")
        year, month, label = _parse_period(str(period_raw))

        return RegularityRecord(
            year=year,
            month=month,
            period_label=label,
            train_type=TrainType.TER,
            region=raw.get("region") or raw.get("nom_region"),
            punctuality_rate=raw.get("taux_de_regularite") or raw.get("regularite"),
            trains_programmed=raw.get("nombre_de_trains_programmes"),
            trains_cancelled=raw.get("nombre_de_trains_annules"),
            trains_late=raw.get("nombre_trains_retard") or raw.get("nombre_de_trains_en_retard"),
            comment=raw.get("commentaires"),
            source_id=source_id,
        )
    except Exception as exc:
        logger.debug(f"Ligne TER ignorée : {exc}")
        return None


def _parse_transilien(raw: dict, source_id: str) -> Optional[RegularityRecord]:
    """Parse un enregistrement du dataset Transilien."""
    try:
        period_raw = raw.get("date") or raw.get("periode") or raw.get("annee_mois", "")
        year, month, label = _parse_period(str(period_raw))

        return RegularityRecord(
            year=year,
            month=month,
            period_label=label,
            train_type=TrainType.TRANSILIEN,
            ligne=raw.get("ligne") or raw.get("code_ligne"),
            punctuality_rate=raw.get("taux_de_ponctualite") or raw.get("ponctualite") or raw.get("regularite"),
            trains_programmed=raw.get("nombre_de_trains_programmes"),
            trains_cancelled=raw.get("nombre_de_trains_annules"),
            trains_late=raw.get("nombre_de_trains_en_retard"),
            comment=raw.get("commentaires"),
            source_id=source_id,
        )
    except Exception as exc:
        logger.debug(f"Ligne Transilien ignorée : {exc}")
        return None


def _parse_intercites(raw: dict, source_id: str) -> Optional[RegularityRecord]:
    """Parse un enregistrement du dataset Intercités."""
    try:
        period_raw = raw.get("date") or raw.get("periode") or raw.get("annee_mois", "")
        year, month, label = _parse_period(str(period_raw))

        return RegularityRecord(
            year=year,
            month=month,
            period_label=label,
            train_type=TrainType.INTERCITES,
            axe=raw.get("axe") or raw.get("liaison"),
            punctuality_rate=raw.get("taux_de_regularite") or raw.get("regularite"),
            trains_programmed=raw.get("nombre_de_trains_programmes"),
            trains_cancelled=raw.get("nombre_de_trains_annules"),
            trains_late=raw.get("nombre_de_trains_en_retard_a_l_arrivee"),
            comment=raw.get("commentaires"),
            source_id=source_id,
        )
    except Exception as exc:
        logger.debug(f"Ligne Intercités ignorée : {exc}")
        return None


# Dispatch parser par source_id
_PARSERS = {
    "tgv":        _parse_tgv,
    "tgv_axe":    _parse_tgv_axe,
    "ter":        _parse_ter,
    "transilien": _parse_transilien,
    "intercites": _parse_intercites,
}


# ---------------------------------------------------------------------------
# Fetch HTTP d'un dataset
# ---------------------------------------------------------------------------

async def _fetch_dataset(
    client:    httpx.AsyncClient,
    source_id: str,
    source:    dict,
) -> list[RegularityRecord]:
    """
    Télécharge et parse un dataset de régularité depuis l'API ODS SNCF.

    Returns:
        Liste de RegularityRecord validés
    """
    logger.info(f"Chargement {source['name']}...")

    try:
        response = await client.get(
            source["url"],
            params=source["params"],
            timeout=REGULARITY_TIMEOUT,
        )
        response.raise_for_status()
        raw_data = response.json()

    except httpx.TimeoutException:
        logger.error(f"Timeout sur {source['name']}")
        return []
    except httpx.HTTPStatusError as exc:
        logger.error(f"HTTP {exc.response.status_code} sur {source['name']}")
        return []
    except Exception as exc:
        logger.error(f"Erreur fetch {source['name']}: {exc}")
        return []

    # L'API ODS peut retourner une liste directe ou un dict avec "results"
    if isinstance(raw_data, list):
        rows = raw_data
    elif isinstance(raw_data, dict):
        rows = raw_data.get("results", raw_data.get("records", [raw_data]))
    else:
        logger.warning(f"Format inattendu pour {source['name']}")
        return []

    parser = _PARSERS[source_id]
    records = []
    for row in rows:
        # L'API ODS peut encapsuler les champs dans "fields"
        if "fields" in row:
            row = row["fields"]
        parsed = parser(row, source_id)
        if parsed:
            records.append(parsed)

    logger.success(f"{source['name']} → {len(records)} enregistrements chargés")
    return records


# ---------------------------------------------------------------------------
# Écriture dans DuckDB
# ---------------------------------------------------------------------------

def _ensure_regularity_table(conn) -> None:
    """Crée la table bronze.regularity_raw si elle n'existe pas."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.regularity_raw (
            id                VARCHAR   PRIMARY KEY,
            year              INTEGER   NOT NULL,
            month             INTEGER   NOT NULL,
            period_label      VARCHAR   NOT NULL,
            train_type        VARCHAR   NOT NULL,
            axe               VARCHAR,
            region            VARCHAR,
            ligne             VARCHAR,
            punctuality_rate  DOUBLE,
            trains_programmed INTEGER,
            trains_cancelled  INTEGER,
            trains_late       INTEGER,
            comment           VARCHAR,
            source_id         VARCHAR   NOT NULL,
            loaded_at         TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_regularity_period
            ON bronze.regularity_raw (year, month, train_type)
    """)


def _write_regularity_records(records: list[RegularityRecord]) -> dict:
    """
    Persiste les RegularityRecord dans bronze.regularity_raw.
    INSERT OR IGNORE : ne réécrit pas les données déjà présentes.

    Returns:
        Stats d'écriture
    """
    stats = {"inserted": 0, "skipped": 0, "errors": 0}

    with get_db() as conn:
        _ensure_regularity_table(conn)

        for rec in records:
            # PK : combinaison unique type + période + segment
            segment = rec.axe or rec.region or rec.ligne or "national"
            pk = f"{rec.source_id}_{rec.period_label}_{segment}".replace(" ", "_").replace("/", "-")

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO bronze.regularity_raw (
                        id, year, month, period_label, train_type,
                        axe, region, ligne,
                        punctuality_rate, trains_programmed,
                        trains_cancelled, trains_late,
                        comment, source_id, loaded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    pk,
                    rec.year,
                    rec.month,
                    rec.period_label,
                    rec.train_type.value,
                    rec.axe,
                    rec.region,
                    rec.ligne,
                    rec.punctuality_rate,
                    rec.trains_programmed,
                    rec.trains_cancelled,
                    rec.trains_late,
                    rec.comment,
                    rec.source_id,
                    rec.loaded_at,
                ])
                stats["inserted"] += 1
            except Exception as exc:
                logger.debug(f"Ligne ignorée (doublon probable) : {exc}")
                stats["skipped"] += 1

    return stats


# ---------------------------------------------------------------------------
# Fonction principale publique
# ---------------------------------------------------------------------------

async def load_all_regularity_data(
    sources: list[str] = None,
) -> dict:
    """
    Charge tous les datasets de régularité depuis l'Open Data SNCF.

    Args:
        sources: Liste des source_id à charger (défaut : toutes).
                 Ex: ["tgv", "ter"] pour ne charger que TGV et TER.

    Returns:
        Dictionnaire de stats par source.
    """
    if sources is None:
        sources = list(REGULARITY_SOURCES.keys())

    all_stats = {}
    all_records: list[RegularityRecord] = []

    async with httpx.AsyncClient() as client:
        # Fetch de toutes les sources en parallèle
        tasks = {
            source_id: _fetch_dataset(client, source_id, REGULARITY_SOURCES[source_id])
            for source_id in sources
            if source_id in REGULARITY_SOURCES
        }
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        for source_id, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"Erreur sur {source_id}: {result}")
                all_stats[source_id] = {"inserted": 0, "skipped": 0, "errors": 1}
            else:
                all_records.extend(result)
                all_stats[source_id] = {"fetched": len(result)}

    # Écriture groupée en DuckDB
    if all_records:
        write_stats = _write_regularity_records(all_records)
        logger.success(
            f"Régularité historique chargée → "
            f"{write_stats['inserted']} insérés | "
            f"{write_stats['skipped']} doublons ignorés | "
            f"{write_stats['errors']} erreurs"
        )
        all_stats["_total"] = write_stats
    else:
        logger.warning("Aucun enregistrement de régularité chargé")

    return all_stats


def get_regularity_summary() -> dict:
    """
    Retourne un résumé des données de régularité chargées.
    Utilisé par FastAPI /governance/catalog et le dashboard.
    """
    with get_db(read_only=True) as conn:
        try:
            rows = conn.execute("""
                SELECT
                    train_type,
                    COUNT(*)                    AS total_records,
                    MIN(period_label)           AS earliest,
                    MAX(period_label)           AS latest,
                    ROUND(AVG(punctuality_rate), 2) AS avg_punctuality
                FROM bronze.regularity_raw
                GROUP BY train_type
                ORDER BY train_type
            """).fetchall()

            return {
                row[0]: {
                    "total_records":   row[1],
                    "earliest":        row[2],
                    "latest":          row[3],
                    "avg_punctuality": row[4],
                }
                for row in rows
            }
        except Exception:
            return {}


def get_punctuality_trend(
    train_type: str,
    year_from:  int = 2020,
    region:     str = None,
    axe:        str = None,
) -> list[dict]:
    """
    Retourne la tendance de ponctualité pour un type de train.
    Utilisé par les graphiques historiques du dashboard.

    Args:
        train_type: "tgv" | "ter" | "transilien" | "intercites"
        year_from:  Année de début (défaut: 2020)
        region:     Filtre région pour TER (optionnel)
        axe:        Filtre axe pour TGV/IC (optionnel)
    """
    filters = ["train_type = ?", "year >= ?"]
    params  = [train_type, year_from]

    if region:
        filters.append("region = ?")
        params.append(region)
    if axe:
        filters.append("axe = ?")
        params.append(axe)

    where = " AND ".join(filters)

    with get_db(read_only=True) as conn:
        try:
            rows = conn.execute(f"""
                SELECT
                    period_label,
                    year,
                    month,
                    ROUND(AVG(punctuality_rate), 2) AS punctuality_rate,
                    SUM(trains_programmed)          AS trains_programmed,
                    SUM(trains_cancelled)           AS trains_cancelled
                FROM bronze.regularity_raw
                WHERE {where}
                GROUP BY period_label, year, month
                ORDER BY year, month
            """, params).fetchall()

            return [
                {
                    "period":             row[0],
                    "year":               row[1],
                    "month":              row[2],
                    "punctuality_rate":   row[3],
                    "trains_programmed":  row[4],
                    "trains_cancelled":   row[5],
                }
                for row in rows
            ]
        except Exception as exc:
            logger.error(f"Erreur get_punctuality_trend: {exc}")
            return []


# ---------------------------------------------------------------------------
# Point d'entrée standalone : python -m ingestion.regularity.loader
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from rich.console import Console
    from rich.table import Table

    console = Console()

    async def _main():
        console.rule("[bold cyan]SNCF Data Observatory — Regularity Loader[/bold cyan]")
        console.print("Chargement des données historiques de régularité SNCF...\n")

        stats = await load_all_regularity_data()

        # Tableau de résultats du chargement
        load_table = Table(title="Résultats du chargement", header_style="bold cyan")
        load_table.add_column("Source",   style="dim", width=20)
        load_table.add_column("Fetchés",  justify="right", width=10)
        load_table.add_column("Insérés",  justify="right", width=10)

        for source_id, s in stats.items():
            if source_id == "_total":
                continue
            load_table.add_row(
                source_id,
                str(s.get("fetched", "—")),
                str(s.get("inserted", "—")),
            )

        console.print(load_table)

        if "_total" in stats:
            t = stats["_total"]
            console.print(
                f"\n  Total : [green]{t['inserted']} insérés[/green] | "
                f"[yellow]{t['skipped']} doublons ignorés[/yellow] | "
                f"[red]{t['errors']} erreurs[/red]"
            )

        # Résumé de ce qui est maintenant en base
        console.print()
        summary = get_regularity_summary()
        if summary:
            summary_table = Table(title="Données en base", header_style="bold cyan")
            summary_table.add_column("Type train",      width=14)
            summary_table.add_column("Enregistrements", justify="right", width=16)
            summary_table.add_column("Depuis",          width=10)
            summary_table.add_column("Jusqu'à",         width=10)
            summary_table.add_column("Moy. ponctualité",justify="right", width=16)

            for train_type, s in summary.items():
                avg = s["avg_punctuality"]
                color = "green" if avg and avg >= 85 else "yellow" if avg and avg >= 75 else "red"
                summary_table.add_row(
                    train_type,
                    str(s["total_records"]),
                    s["earliest"] or "—",
                    s["latest"] or "—",
                    f"[{color}]{avg}%[/{color}]" if avg else "—",
                )

            console.print(summary_table)

        console.print("\n[green]Chargement terminé.[/green]")

    asyncio.run(_main())