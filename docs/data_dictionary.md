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

