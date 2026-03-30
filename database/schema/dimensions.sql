-- database/schema/dimensions.sql

-- 1. Geography Dimension
-- This is built from your nuts_istat.csv
CREATE OR REPLACE TABLE dim_geography AS
SELECT 
    nuts_code,
    nuts_level,
    nuts_name_it,
    nuts_name_en,
    region_name,
    macro_area
FROM read_csv_auto('processing/mappings/nuts_istat.csv');

-- 2. Time Dimension
-- A simple table to help PowerBI handle years consistently
CREATE OR REPLACE TABLE dim_time AS
SELECT 
    DISTINCT time AS year,
    CAST(time AS INTEGER) as year_int
FROM 'aiccon-data/processed/fact_social_economy.parquet';

-- 3. Source Dimension
-- Built from your domain_sources.csv
CREATE OR REPLACE TABLE dim_source AS
SELECT 
    source_id,
    source_name,
    priority,
    update_frequency
FROM read_csv_auto('processing/mappings/domain_sources.csv');