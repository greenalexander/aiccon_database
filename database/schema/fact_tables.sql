-- database/schema/fact_tables.sql

-- The core data table for Social Economy
CREATE OR REPLACE TABLE fact_social_economy (
    time            VARCHAR,    -- The year (e.g., "2022")
    nuts_code       VARCHAR,    -- The geographical key
    nuts_level      INTEGER,    -- 0, 1, 2, or 3
    unified_category VARCHAR,   -- From your legal_form_map
    unified_category_en VARCHAR,
    value           DOUBLE,     -- The actual statistic (count, employees, etc.)
    source_name     VARCHAR,    -- ISTAT or Eurostat
    source_priority INTEGER     -- Used for de-duplication
);

-- We also create an index on nuts_code and time to make PowerBI filters lightning fast
CREATE INDEX idx_geo_time ON fact_social_economy (nuts_code, time);