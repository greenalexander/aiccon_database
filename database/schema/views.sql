-- database/schema/views.sql

-- This is the main view for the Social Economy Dashboard
CREATE OR REPLACE VIEW v_social_economy_comprehensive AS
SELECT 
    f.time,
    -- Geography Details
    g.nuts_name_it AS territory_name,
    g.region_name,
    g.macro_area,
    f.nuts_level,
    f.nuts_code,
    
    -- Category Details
    f.unified_category,
    f.unified_category_en,
    
    -- Values
    f.value,
    
    -- Metadata
    f.source_name,
    s.update_frequency
FROM fact_social_economy f
LEFT JOIN dim_geography g ON f.nuts_code = g.nuts_code
LEFT JOIN dim_source s ON f.source_name = s.source_name;

-- A specific view for "Only Italy" at Provincial/Regional level
CREATE OR REPLACE VIEW v_social_economy_italy AS
SELECT * FROM v_social_economy_comprehensive
WHERE nuts_code LIKE 'IT%';