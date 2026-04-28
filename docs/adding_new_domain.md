# Data Dictionary: Social Economy Domain
This documentation summarizes the architectural pattern for adding new domains to the pipeline. By following this structure, you isolate domain-specific logic while leveraging the shared infrastructure built in the initial phases.

## Summary of Work
For each new domain, you typically create **two sets of new Python files** and make small additions to **five existing files**. Everything else is shared infrastructure you never touch.

---

## 1. New Files (Domain-Specific)
*Create these from scratch for every new domain (e.g., using `immigration` as the example).*

### Data Ingestion
* **`ingestion/api_sources/immigration/`**
    * `__init__.py`: Standard package initializer.
    * `eurostat.py`: New fetcher for Eurostat immigration datasets.
    * `istat.py`: New fetcher for ISTAT immigration datasets.

### Integration Logic
* **`processing/integrate/`**
    * `merge_immigration.py`: The domain-specific merger (equivalent to `merge_social_economy.py`). 

---

## 2. Files to Edit (Integration)
*Add a small amount of configuration or logic to these existing files.*

* **`processing/pipeline.py`**
    * Import `merge_immigration`.
    * Add `"immigration": merge_immigration` to the `DOMAIN_PROCESSORS` dictionary.
* **`config/settings.yaml`**
    * Set `immigration: enabled: true`.
    * Add `istat.immigration` and `eurostat.immigration` dataset lists.
* **`processing/mappings/domain_sources.csv`**
    * Add rows for the new immigration sources.

---

## 3. Database Layer
*Determined by whether the new domain requires its own storage.*

* **`database/schema/fact_tables.sql`**
    * Add `CREATE TABLE fact_immigration (...)`.
* **`database/schema/views.sql`**
    * (Optional) Add convenience views for the new domain.
* **`database/build_db.py`**
    * Add the new fact table to the build sequence.

---

## 4. Maintenance & Shared Infrastructure

### Files to Extend (If needed)
You may need to add rows to these mapping files, but the logic remains the same:
* `processing/mappings/nuts_istat.csv` (Geography - usually already complete)
* `processing/mappings/legal_form_map.csv` (Legal entities)
* `processing/mappings/nace_labels.csv` (Economic activities)

### Shared Files (Do Not Touch)
These files are domain-agnostic and should remain unchanged:

| Category | File | Role |
| :--- | :--- | :--- |
| **Loaders** | `ingestion/loaders/base_loader.py` | Shared base logic |
| **Loaders** | `ingestion/loaders/sharepoint_uploader.py` | Shared uploader |
| **Harmonization** | `processing/harmonise/nuts_mapper.py` | Universal NUTS mapping |
| **Harmonization** | `processing/harmonise/legal_form.py` | Universal legal form logic |
| **Database** | `database/schema/dimensions.sql` | Shared dimension tables |
| **Entry Point** | `run_pipeline.py` | Shared execution entry point |
