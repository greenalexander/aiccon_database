# aiccon-data

Contextual statistics platform for AICCON research and projects. Integrates European, national (Italy), and subnational data across the thematic areas of AICCON's work — social economy, welfare, immigration, social impact, and sustainable development — and makes them available through PowerBI dashboards.

---

## Purpose

Researchers and project staff at AICCON regularly need statistical context for their work: size of the third sector in a given region, poverty rates, immigration flows, SDG indicator progress, social spending trends. This project collects that data from authoritative public sources, integrates it into a unified database keyed by geography and time, and surfaces it in PowerBI for filtering and exploration.

The database is maintained by one person and updated approximately monthly via automated Python scripts. Colleagues access it through PowerBI only — no coding required on their end.

---

## Thematic domains

Each domain is a self-contained collection of datasets and indicators, stored as a separate set of parquet files and exposed as its own fact table in the database. All domains share the same geography, time, and source dimension tables.

| Domain | Description | Key sources |
|---|---|---|
| **Social economy** | Third sector organisations (cooperative sociali, ODV, APS, fondazioni, ETS), employment, revenue, legal form breakdowns | ISTAT, RUNTS, Registro Imprese, Eurostat |
| **Welfare and social policies** | Social spending, public social services, long-term care, disability, childcare | ISTAT, Eurostat, OECD |
| **Poverty and inequality** | At-risk-of-poverty rates, material deprivation, income inequality (Gini), food poverty | ISTAT (BES), Eurostat (EU-SILC), OECD |
| **Immigration** | Resident foreign population, nationalities, work permits, asylum requests, integration indicators | ISTAT, Ministero dell'Interno, Eurostat |
| **Labour market** | Employment and unemployment rates, precarious work, gender gaps, youth employment | ISTAT (RCFL), Eurostat (LFS) |
| **Sustainable development (SDGs)** | Italy and EU progress on Agenda 2030 indicators, mapped to AICCON research focus areas | ISTAT (SDGs), Eurostat (SDG monitoring) |
| **Social impact and civil society** | Volunteering rates, social capital indicators, civic engagement, philanthropic giving | ISTAT, CSVnet, Fondazioni Italia |
| **Housing and urban welfare** | Housing affordability, homelessness, social housing, urban regeneration indicators | ISTAT, Eurostat, Federcasa |

New domains can be added without modifying the existing database structure. See [Adding a new domain](#adding-a-new-domain) below.

---

## Data sources

### APIs (automated)

| Source | Access | Coverage |
|---|---|---|
| Eurostat | SDMX / JSON API | EU27, NUTS0–2, all available years |
| ISTAT | SDMX API (dati.istat.it) | Italy, NUTS0–3, all available years |
| ECB | REST API | Euro area macro context |
| UN / OECD | SDMX / JSON API | International comparisons |

### Manual downloads (not automated)

| Source | Coverage | Notes |
|---|---|---|
| RUNTS | Italy | Registry of third sector entities (post-2022); download from MLPS portal |
| Registro Imprese / Camere di Commercio | Italy, provincial | Cooperative register; export from InfoCamere |
| Agenzia delle Entrate | Italy | Fiscal data on nonprofits; annual publication |
| Ministero dell'Interno | Italy | Asylum and permit data not in ISTAT API |

Manual files are stored in `ingestion/manual_sources/` and are **not committed to Git**. Record the download date, source URL, and file description in `docs/source_log.md` each time they are refreshed.

---

## Repository structure

```
aiccon-data/
├── ingestion/
│   ├── api_sources/          # One script per API (eurostat.py, istat.py, ecb.py, ...)
│   ├── manual_sources/       # Raw downloaded files — gitignored
│   ├── loaders/              # base_loader.py, sharepoint_uploader.py
│   └── schemas/              # Raw layer validation schemas (pandera)
│
├── processing/
│   ├── harmonise/            # nuts_mapper.py, nace_mapper.py, legal_form_normaliser.py
│   ├── integrate/            # merge_sources.py, priority_resolver.py
│   ├── mappings/             # Mapping tables as plain CSV files:
│   │   ├── nuts_istat.csv    #   NUTS <-> ISTAT municipality/province codes
│   │   ├── legal_form_map.csv#   Italian legal forms <-> NACE / Eurostat categories
│   │   ├── sdg_indicators.csv#   SDG goal+target <-> ISTAT/Eurostat indicator codes
│   │   └── domain_sources.csv#   Which sources feed which domain, with priority rank
│   └── pipeline.py           # Orchestrates the full processing run
│
├── database/
│   ├── schema/               # star_schema.sql, create_views.sql
│   ├── build_db.py           # Reads parquet from SharePoint -> builds .duckdb file
│   └── tests/                # Row count checks, null checks, key uniqueness
│
├── config/
│   ├── settings.yaml         # SharePoint paths, API endpoints, domain scope config
│   └── .env.example          # Credential template (copy to .env, never commit)
│
├── docs/
│   ├── data_dictionary.md    # Field definitions and units for every table
│   ├── source_log.md         # Manual download dates and source URLs
│   └── decisions.md          # Log of mapping choices and priority rules, with rationale
│
├── run_pipeline.py           # Entry point: ingest -> process -> build database
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Setup

**Requirements**: Python 3.11+

```bash
git clone 
cd aiccon-data
pip install -r requirements.txt
cp config/.env.example config/.env
# Edit config/.env — add SharePoint credentials and any API tokens
```

Set your SharePoint folder paths and the domains you want to activate in `config/settings.yaml`.

---

## Running the pipeline

```bash
python run_pipeline.py
```

Runs the full pipeline in sequence:
1. **Ingest** — fetches from all active APIs; reads any new manual source files
2. **Process** — harmonises geography (NUTS/ISTAT codes), maps legal forms and indicator codes, resolves source priority, writes parquet to SharePoint
3. **Build** — reads processed parquet from SharePoint, builds the DuckDB star schema, exports `.duckdb` for PowerBI

Run a single stage or domain:
```bash
python run_pipeline.py --stage ingest
python run_pipeline.py --stage process
python run_pipeline.py --stage database

python run_pipeline.py --domain social_economy
python run_pipeline.py --domain immigration
```

**Update frequency**: monthly. Every parquet file includes an `extracted_at` timestamp column.

---

## Data architecture

### Storage layers within Sharepoint

| Layer | Format | SharePoint path | Description |
|---|---|---|---|
| Raw | Parquet | `/raw/{domain}/` | Source data as-received, with `extracted_at`. Never modified after writing. |
| Processed | Parquet | `/processed/{domain}/` | Harmonised: unified NUTS keys, normalised categories, deduplicated, priority-resolved |
| Database | DuckDB (`.duckdb`) | `/database/` | Star schema consumed by PowerBI |

### Database schema (star model)

**Shared dimension tables** — written once, reused by every domain:

| Table | Key fields | Description |
|---|---|---|
| `dim_geography` | `geo_key`, `nuts_code`, `istat_code`, `name_it`, `name_en`, `level` | All territories from comune to EU country |
| `dim_time` | `time_key`, `year`, `reference_period` | Annual and sub-annual periods |
| `dim_source` | `source_key`, `source_name`, `access_type`, `priority`, `update_freq` | Source registry with priority ranking |

**Domain fact tables** — one per thematic area, all sharing the same dimension tables:

| Table | Domain | Additional dimensions |
|---|---|---|
| `fact_social_economy` | Social economy | legal_form, nace_code |
| `fact_welfare` | Welfare and social policies | service_type, beneficiary_group |
| `fact_poverty` | Poverty and inequality | population_group, deprivation_type |
| `fact_immigration` | Immigration | nationality, permit_type |
| `fact_labour` | Labour market | gender, age_group, contract_type |
| `fact_sdg` | SDGs | sdg_goal, sdg_target |
| `fact_civil_society` | Social impact / civil society | org_type |
| `fact_housing` | Housing and urban welfare | tenure_type |

All fact tables share the same core fields: dimension foreign keys, `indicator_code`, `value`, `unit`, and `extracted_at`.

### Key mapping decisions

**Geography**: NUTS codes are the universal join key. ISTAT province and municipality codes are mapped to NUTS3 via `mappings/nuts_istat.csv`. Data available only at comune level is stored at that granularity and aggregated upward for higher levels.

**Legal form**: Italian third sector categories (cooperativa sociale, ODV, APS, etc.) have no direct Eurostat equivalent. They are mapped to NACE Rev. 2 codes for European comparisons and retained verbatim for domestic data. See `mappings/legal_form_map.csv`.

**Source priority** when the same indicator appears in multiple sources:
1. ISTAT — most granular for Italy; preferred for all domestic indicators
2. Eurostat — EU comparisons and gaps in ISTAT coverage
3. RUNTS / Registro Imprese — entity counts, legal form breakdowns
4. OECD / UN — international comparisons, SDG baselines
5. ECB — macro context only

Full rationale for individual decisions is logged in `docs/decisions.md`.

---

## Adding a new domain

1. Add a script to `ingestion/api_sources/` (or a folder to `ingestion/manual_sources/`)
2. Add a validation schema to `ingestion/schemas/`
3. Add any new category mappings to `processing/mappings/`
4. Add a new fact table definition to `database/schema/star_schema.sql`
5. Register the domain and its sources in `config/settings.yaml`
6. Document indicators, units, and sources in `docs/data_dictionary.md`
7. Log the decision (including sources evaluated and not chosen) in `docs/decisions.md`

---

## What is not in this database

This database provides **contextual statistics** — aggregate indicators from official public sources. It does not contain:
- Microdata or individual-level records
- Data not available from public APIs or official publications
- Real-time data (updates are monthly at most)
- Internal AICCON project data or client information

---

## Maintainer

**[Alexander Green]** — AICCON  
Last pipeline run: see SharePoint `/database/pipeline_log.json`  
Contact: [alexander.green@unibo.it]



--- 
Build plan — social economy pipeline
Phase 0 — Foundations (do once, reused by all future domains)

Step 1: requirements.txt and .gitignore
Step 2: config/settings.yaml and config/.env.example
Step 3: processing/mappings/ — the three core CSV mapping tables
Step 4: ingestion/loaders/base_loader.py — shared loading utilities 
Step 5: ingestion/loaders/sharepoint_uploader.py — writes parquet to SharePoint

Phase 1 — Ingest (social economy data only)

Step 6: ingestion/api_sources/social_economy/eurostat.py — pull NACE Q + S94 employment at NUTS0/NUTS2 for all EU countries 
Step 7: ingestion/api_sources/social_economy/istat.py — pull ISTAT nonprofit census and cooperative data at NUTS3, with gender 

ALEX:
-- update readme and yaml on the basis of eurostat: changed datasets and filters for eurostat. 
-- Same thing with readme and yaml for istat


Phase 2 — Process

Step 8: processing/harmonise/nuts_mapper.py — ISTAT codes → NUTS codes [Alex check]
Step 9: processing/harmonise/legal_form.py — Italian legal forms → unified categories [Alex check]
Step 10: processing/integrate/merge_sources.py — combine Eurostat + ISTAT with priority rules [Alex check]
Step 11: processing/pipeline.py — wire steps 6–10 together for social economy [Alex check]

Phase 3 — Database

Step 12: database/schema/dimensions.sql — dim_geography, dim_time, dim_source [Alex check]
Step 13: database/schema/fact_tables.sql — fact_social_economy [Alex check]
Step 14: database/schema/views.sql — pre-built views useful for PowerBI [Alex check]
Step 15: database/build_db.py — reads processed parquet, builds .duckdb [Alex check]
Step 16: database/tests/ — basic integrity checks [Alex check]

Phase 4 — Entry point + docs

Step 17: run_pipeline.py — single command to run everything [Alex check]
Step 18: docs/data_dictionary.md — field definitions for the social economy tables [Alex check]
Step 19: docs/source_log.md and docs/decisions.md — templates to fill in [Alex check]