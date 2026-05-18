# Data Dictionary: Social Economy Domain

## Table: `v_social_economy_comprehensive` (Main View)
The primary view for all PowerBI reports. It joins the fact table with geography and source dimensions.

| Field | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `time` | VARCHAR | The reference year for the data. | "2022" |
| `territory_name` | VARCHAR | Italian name of the NUTS region or province. | "Bologna" |
| `region_name` | VARCHAR | The parent region (Region) of the territory. | "Emilia-Romagna" |
| `macro_area` | VARCHAR | Italian macro-area (NUTS1). | "Nord-Est" |
| `nuts_level` | INT | Granularity: 0 (National), 2 (Regional), 3 (Provincial). | 3 |
| `nuts_code` | VARCHAR | Unique European NUTS identifier. | "ITH55" |
| `unified_category` | VARCHAR | AICCON category (from mapping). | "Cooperativa sociale" |
| `unified_category_en`| VARCHAR | English translation of the category. | "Social cooperative" |
| `value` | DOUBLE | The actual metric (Headcount, Count of orgs, etc.). | 1450.0 |
| `source_name` | VARCHAR | Origin of the data (ISTAT, Eurostat). | "ISTAT" |
| `update_frequency` | VARCHAR | How often the source is updated. | "Annual" |

## Metrics Definitions
- **Nonprofit Count**: Number of legal entities registered in the specific year.
- **Employment**: Number of paid employees (Headcount). For Eurostat, this is expressed in thousands.
- **Volunteers**: Number of non-paid staff (primarily from ISTAT Census).



# Data Dictionary

Definitions, units, and notes for every field in the aiccon-data database.
Updated whenever a new domain or dataset is added.

Last updated: May 2026

---

## Shared dimension tables

These tables are used by every domain. They aren't directly queried in PowerBI — they are joined automatically through the analytical views (`vw_*`).

### `dim_geography`

| Column | Type | Description |
|---|---|---|
| `geo_key` | integer | Surrogate primary key |
| `nuts_code` | text | NUTS code (e.g. `IT`, `ITH5`, `ITH55`). For non-EU countries of origin (future immigration domain), ISO 3166-1 alpha-3 is used instead |
| `nuts_level` | integer | 0 = country, 1 = macro-region, 2 = region (NUTS2), 3 = province (NUTS3) |
| `nuts_name_it` | text | Italian place name |
| `nuts_name_en` | text | English place name |
| `istat_code` | text | ISTAT territorial code (e.g. `037` for Bologna province) |
| `country_code` | text | ISO 3166-1 alpha-2 (e.g. `IT`, `FR`, `DE`) |
| `region_name` | text | Italian region name (null for non-Italian rows) |
| `macro_area` | text | Italian macro-area: `Nord-Ovest`, `Nord-Est`, `Centro`, `Sud`, `Isole` (null for non-Italian rows) |
| `municipality_type` | text | Municipality size class from ISTAT surveys (e.g. `>250k`, `50-250k`, `<10k`). Populated only for rows representing a size class rather than a specific territory |
| `geo_source` | text | `nuts_istat_csv` (from mapping table), `eurostat_auto` (inserted automatically when Eurostat data contained an unknown code), `manual` |
| `is_active` | boolean | `false` for discontinued NUTS codes from previous vintages |

### `dim_time`

| Column | Type | Description |
|---|---|---|
| `time_key` | integer | Surrogate primary key |
| `year` | integer | Reference year (e.g. `2021`) |
| `period_type` | text | `A` = annual (all current data). `Q` and `M` reserved for future sub-annual data |
| `period_label` | text | Human-readable label (e.g. `2021`) |
| `reference_period` | text | Extended label where useful (e.g. `2021 (Census)`) |

### `dim_source`

| Column | Type | Description |
|---|---|---|
| `source_key` | integer | Surrogate primary key |
| `source_id` | text | Short identifier matching `domain_sources.csv` (e.g. `istat_85_84_DF_DCSA_VOLON1_1`) |
| `source_name` | text | English source name |
| `source_name_it` | text | Italian source name |
| `domain` | text | Primary thematic domain this source belongs to |
| `access_type` | text | `api` or `manual` |
| `priority` | integer | Source priority for deduplication: 1 = highest (ISTAT), 2 = Eurostat, 3 = RUNTS/Camere di Commercio, 4 = Agenzia delle Entrate |
| `update_frequency` | text | How often the source publishes new data |
| `territorial_levels` | text | Semicolon-separated list of available territorial levels (e.g. `NUTS0;NUTS2`) |
| `temporal_coverage` | text | Years covered (e.g. `2015-present`) |
| `notes` | text | Important caveats about coverage, methodology, or known issues |

### `dim_legal_form`

| Column | Type | Description |
|---|---|---|
| `legal_form_key` | integer | Surrogate primary key |
| `unified_category` | text | Unified category used as the join key across sources (e.g. `cooperativa_sociale`, `odv`, `aps`, `fondazione`) |
| `unified_category_en` | text | English label |
| `nace_primary` | text | Primary NACE Rev. 2 mapping (e.g. `Q87;Q88` for social cooperatives) |
| `ets_classification` | text | `ets` = registered in RUNTS under D.Lgs. 117/2017; `ets_eligible` = can register but may not be; `non_ets` = excluded from ETS scope |
| `source_system` | text | Source this row came from: `istat`, `runts`, `camere_commercio`, `eurostat` |
| `source_code` | text | Original code in the source system (e.g. `COOP_SOC_A`) |
| `source_label_it` | text | Original Italian label in the source system |

### `dim_indicator`

| Column | Type | Description |
|---|---|---|
| `indicator_key` | integer | Surrogate primary key |
| `indicator_code` | text | Short code used in fact tables (e.g. `volunteering_rate`) |
| `label_it` | text | Italian label for PowerBI display |
| `label_en` | text | English label |
| `unit_default` | text | Most common unit for this indicator (see unit values below) |
| `domain` | text | Primary domain this indicator belongs to |
| `notes` | text | Caveats, methodology notes, or cross-references |

---

## Social economy fact table — `fact_social_economy` / `vw_social_economy`

### Core fields (present in every row)

| Column | Type | Description |
|---|---|---|
| `fact_key` | integer | Surrogate primary key |
| `geo_key` | integer | Foreign key → `dim_geography` |
| `time_key` | integer | Foreign key → `dim_time` |
| `source_key` | integer | Foreign key → `dim_source` |
| `indicator_key` | integer | Foreign key → `dim_indicator` |
| `value` | float | The numeric observation |
| `unit` | text | Unit of measurement for this specific row (see unit reference below) |
| `dataset_code` | text | Original dataset identifier for traceability (e.g. `85_84_DF_DCSA_VOLON1_1`) |
| `extracted_at` | timestamp | UTC timestamp when the raw data was fetched from the API |

### Legal form fields (populated for organisational datasets; null for survey data)

| Column | Type | Description |
|---|---|---|
| `legal_form_key` | integer | Foreign key → `dim_legal_form`. Null for NACE-based and survey datasets |
| `ets_classification` | text | `ets` / `ets_eligible` / `non_ets`. Null where legal form is not a dimension |

### NACE fields (populated for Eurostat datasets; null for survey data)

| Column | Type | Description |
|---|---|---|
| `nace_code` | text | NACE Rev. 2 / ATECO 2007 code (e.g. `Q87`, `P85`, `O-U`). Note: `O-U` is an aggregate used in `nama_10r_3empers` at NUTS2 level — Q, P, and S94 are not separable at this territorial level |
| `nace_label_en` | text | English label from `nace_labels.csv` |

### Demographic fields (populated where available; null otherwise)

| Column | Type | Description |
|---|---|---|
| `gender` | text | `male`, `female`, or `total`. Null for Eurostat regional employment datasets which do not provide a gender breakdown |
| `age_group` | text | Age group label as provided by ISTAT (e.g. `15-34`, `35-64`, `65+`). Null where age is not a dimension |

### Volunteering survey dimensions
*(from ISTAT Indagine sul Volontariato, datasets `85_84_DF_DCSA_VOLON1_*`)*

| Column | Type | Description |
|---|---|---|
| `volunteering_form` | text | `ORGVOL` = organised volunteering (through an organisation); `DIRVOL` = direct/informal volunteering; `total` = both combined |
| `activity_type` | text | Type of activity: emergency rescue, social assistance, culture, sport, tutoring, environmental protection, etc. |
| `years_active` | text | Duration of volunteering: `<1yr`, `1-2`, `3-5`, `6-10`, `>10` years |
| `education` | text | Highest educational qualification of the volunteer |
| `labour_status` | text | Labour market status: employed, unemployed, retired, student, homemaker, etc. |
| `household_size` | text | Number of people in the volunteer's household |
| `econ_resources` | text | Self-assessed household economic resources: adequate, scarce, insufficient |
| `municipality_type` | text | Size class of municipality of residence |

### Organised volunteering dimensions
*(from ISTAT Indagine sul Volontariato, datasets `85_171_DF_DCSA_VOLON_ORG1_*`)*

| Column | Type | Description |
|---|---|---|
| `org_sector` | text | Sector of the host organisation: sport, culture, social assistance, civil protection, health, environment, education, etc. |
| `org_type` | text | Type of host organisation: ODV, APS, cooperative sociale, religious body, sports club, foundation, other. Maps to legal forms in D.Lgs. 117/2017 |
| `motivation` | text | Primary motivation for volunteering: altruism, faith, social belonging, personal enrichment, skills development, reciprocity, etc. |
| `personal_impact` | text | Self-reported personal benefit: new friendships, sense of usefulness, new skills, improved wellbeing, civic engagement, etc. Relevant for SROI-framework analyses |
| `multi_membership` | text | Whether the volunteer is active in one organisation or multiple organisations simultaneously |

### Associationism dimensions
*(from ISTAT Aspetti della Vita Quotidiana, datasets `83_63_DF_DCCV_AVQ_PERSONE_*`)*

| Column | Type | Description |
|---|---|---|
| `association_type` | text | Type of association the respondent belongs to: cultural, sports, religious, environmental, political, professional, charity, etc. |

---

## Unit reference

| Unit value | Meaning | Source |
|---|---|---|
| `percentage` | Share of population (0–100 scale) | ISTAT surveys |
| `mixed` | Multiple units in the same dataset — read the `unit` column per row | ISTAT `85_84_DF_DCSA_VOLON1_1` only |
| `thousands_persons` | Thousands of employed persons | Eurostat `nama_10r_3empers`, `lfsa_egan22d` |
| `count` | Absolute count of units/organisations | Eurostat `sbs_r_nuts2021` |

---

## Analytical views

These are the objects to connect PowerBI to. They join fact tables to all dimension tables and expose human-readable labels.

| View | Description |
|---|---|
| `vw_social_economy` | Full flat view of all social economy data — use as the primary PowerBI source |
| `vw_se_volunteering_national` | National volunteering rates by year and form — ready for line charts |
| `vw_se_volunteering_regional` | Regional volunteering rates — ready for map visualisations |
| `vw_se_associationism_national` | National association membership rates by year and demographic |
| `vw_se_employment_eu` | Eurostat employment data for EU comparisons |
| `vw_se_local_units_regional` | Local units by NACE and region — organisational density maps |

---

## Known limitations and caveats

**NACE O-U aggregate at NUTS2**: The Eurostat dataset `nama_10r_3empers` provides regional employment only at the O-U aggregate level (public administration, education, health, and social services combined). Sections Q, P, and S94 are not separable at NUTS2 level from this source. For sector-specific employment figures, use `lfsa_egan22d` (national level only) or the ISTAT volunteering survey data.

**Census vs. survey data**: ISTAT volunteering data comes from sample surveys, not a census. Figures are estimates with sampling uncertainty. The associationism data (AVQ) has a larger sample and is more stable year-to-year than the volunteering survey.

**RUNTS coverage**: RUNTS (Registro Unico Terzo Settore) only covers entities registered under D.Lgs. 117/2017 (Codice del Terzo Settore) from 2022 onwards. Pre-2022 entity counts from ISTAT census data are not directly comparable to post-2022 RUNTS counts because the legal categories changed with the Codice.

**Temporal comparability**: The ISTAT nonprofit census runs approximately every 5 years (2001, 2011, 2016, 2021). Employment and organisational count figures from census years should not be interpolated between census rounds.