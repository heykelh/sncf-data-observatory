# 🚄 SNCF Data Observatory

> Plateforme de monitoring temps réel de la qualité des données ferroviaires SNCF — Data Engineering · Data Governance · IA

[![Live Demo](https://img.shields.io/badge/Live%20Demo-sncf--data--observatory.vercel.app-8b5cf6?style=for-the-badge)](https://sncf-data-observatory.vercel.app)
[![API](https://img.shields.io/badge/API-onrender.com-10b981?style=for-the-badge)](https://sncf-data-observatory.onrender.com/docs)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python)](https://python.org)
[![Next.js](https://img.shields.io/badge/Next.js-15-black?style=for-the-badge&logo=next.js)](https://nextjs.org)

---

## 🎯 Objectif

Ce projet démontre la maîtrise complète d'un pipeline Data Engineering de bout en bout :
ingestion de données temps réel, stockage en architecture Medallion, gouvernance des données
(DAMA-DMBOK), exposition via API REST, et visualisation dans un dashboard interactif.

**Cas d'usage** : Monitorer la qualité et la ponctualité du réseau ferroviaire SNCF en temps réel,
tout en appliquant les standards professionnels de gouvernance des données.

---

## 🖥️ Dashboard

| Page | Description |
|------|-------------|
| **Live Monitor** | 1 400+ trains trackés en temps réel · retards · suppressions · alertes officielles |
| **Historique** | 10 ans de régularité mensuelle · TGV · TER · Transilien · Intercités · comparaisons |
| **Gouvernance** | Data Catalog · Data Contracts · Quality Score · Lineage · Rôles & SLA |

---

## 🏗️ Architecture
SNCF Open Data (GTFS-RT)
│
▼
┌─────────────────────────────────────────────┐
│           Pipeline Python                   │
│  fetcher.py → parser.py → writer.py         │
│  APScheduler · httpx · Pydantic v2          │
│  Cycle : toutes les 2 minutes               │
└──────────────┬──────────────────────────────┘
│
▼
┌─────────────────────────────────────────────┐
│         DuckDB — Architecture Medallion     │
│                                             │
│  🥉 Bronze  → données brutes GTFS-RT        │
│  🥈 Silver  → nettoyées · enrichies         │
│  🥇 Gold    → KPIs agrégés · dashboard      │
└──────────────┬──────────────────────────────┘
│
▼
┌─────────────────────────────────────────────┐
│         FastAPI — 14 endpoints REST         │
│  /trips/live · /alerts/active · /kpi/*      │
│  /governance/* · /stops/resolve             │
└──────────────┬──────────────────────────────┘
│
▼
┌─────────────────────────────────────────────┐
│      Dashboard Next.js 15 · Vercel          │
│  Live Monitor · Historique · Gouvernance    │
└─────────────────────────────────────────────┘

---

## 📊 Données

| Source | Type | Volume | Fréquence |
|--------|------|---------|-----------|
| GTFS-RT Trip Updates | Temps réel | ~1 400 trains/cycle | 2 min |
| GTFS-RT Service Alerts | Temps réel | ~400 alertes/cycle | 2 min |
| Régularité TGV | Historique | 132 mois (2013→2026) | Mensuel |
| Régularité TER | Historique | 2 296 enregistrements | Mensuel |
| Ponctualité Transilien | Historique | 1 983 enregistrements | Mensuel |
| Régularité Intercités | Historique | 5 609 enregistrements | Mensuel |
| Référentiel gares GTFS | Statique | 8 843 gares géolocalisées | 1x/déploiement |

**Total : 10 801+ enregistrements historiques · Licence ODbL**

---

## 🛠️ Stack technique

### Backend
- **Python 3.12** — pipeline d'ingestion async
- **DuckDB 0.10** — stockage analytique embarqué (architecture Medallion Bronze/Silver/Gold)
- **FastAPI** — API REST avec validation Pydantic
- **APScheduler** — orchestration des cycles d'ingestion
- **httpx** — client HTTP async
- **gtfs-realtime-bindings** — parsing Protobuf GTFS-RT
- **Loguru · Rich** — logging et monitoring

### Frontend
- **Next.js 15** — framework React avec App Router
- **Tailwind CSS v4** — styling utility-first
- **Recharts** — graphiques interactifs
- **Kanit** — typographie

### Infrastructure
- **Render** — déploiement backend (free tier)
- **Vercel** — déploiement frontend (free tier)
- **GitHub** — CI/CD automatique

---

## 🏛️ Gouvernance des données (DAMA-DMBOK)

Ce projet implémente un cadre de gouvernance complet :

- **Data Catalog** — toutes les sources documentées avec Data Contracts YAML
- **Data Quality Framework** — 4 contrôles automatiques par cycle (fraîcheur, disponibilité, complétude, validité)
- **Data Lineage** — traçabilité complète Bronze → Silver → Gold → Dashboard
- **Rôles & SLA** — Data Owner / Data Steward / Data Consumer avec engagements de service
- **Master Data Management** — référentiel des 8 843 gares SNCF avec codes UIC

---

## 🚀 Lancer en local

### Prérequis
- Python 3.12+
- Node.js 18+
- Git

### Installation

```bash
# Clone
git clone https://github.com/heykelh/sncf-data-observatory.git
cd sncf-data-observatory

# Backend
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac
pip install -r requirements.txt

# Frontend
cd dashboard
npm install
cd ..
```

### Initialisation des données (première fois)

```bash
# Schéma DuckDB
python -m storage.database

# Données historiques de régularité (~1 min)
python -m ingestion.regularity.loader

# Référentiel gares GTFS statique (~3 min · 200MB)
python -m ingestion.gtfs_static.loader
```

### Démarrage

```bash
# Terminal 1 — Backend API + pipeline GTFS-RT
uvicorn api.main:app --reload --port 8000

# Terminal 2 — Dashboard
cd dashboard
npm run dev
```

- Dashboard : http://localhost:3000
- API Swagger : http://localhost:8000/docs

---

## 📁 Structure du projet
sncf-data-observatory/
├── api/
│   └── main.py                 # FastAPI — 14 endpoints REST
├── ingestion/
│   ├── gtfs_rt/
│   │   ├── fetcher.py          # Scheduler + fetch async
│   │   ├── parser.py           # Protobuf → Pydantic
│   │   └── models.py           # Modèles de données
│   ├── gtfs_static/
│   │   └── loader.py           # Référentiel 8 843 gares
│   └── regularity/
│       └── loader.py           # Historique 2013→2026
├── storage/
│   ├── database.py             # DuckDB Bronze/Silver/Gold
│   └── writer.py               # Écriture + quality checks
├── dashboard/
│   ├── app/
│   │   ├── page.tsx            # Live Monitor
│   │   ├── history/page.tsx    # Historique régularité
│   │   └── governance/page.tsx # Gouvernance DAMA-DMBOK
│   └── lib/
│       └── api.ts              # Client API TypeScript
├── render.yaml                 # Config déploiement Render
├── requirements.txt
└── README.md

---

## 🔮 Roadmap V2

- [ ] **Map interactive** — carte des gares avec incidents en temps réel (Maplibre GL)
- [ ] **Agent LangChain** — interroger les données en langage naturel ("En 2018, quels trains ont le plus retardé ?")
- [ ] **Résolution complète des gares** — enrichissement via API Navitia pour 100% des trains
- [ ] **Alertes push** — notifications WebSocket temps réel
- [ ] **dbt** — transformations Silver/Gold documentées

---

## 👤 Auteur

**Michel DUPONT** — Data Engineer / Responsable Data Gouvernance

[![GitHub](https://img.shields.io/badge/GitHub-heykelh-181717?style=flat&logo=github)](https://github.com/heykelh)

---

## 📄 Licence

Données SNCF — Licence ODbL (Open Database License)
Code source — MIT