# Design Decisions & Assumptions

### 1. Priority Rules (ISTAT vs. Eurostat)
**Decision:** When data for the same year/territory exists in both, ISTAT is prioritized.
**Reason:** ISTAT provides deeper granularity for legal forms (Coop Sociali A vs B) which is the core of AICCON's research requirements.

### 2. NACE Filtering for Social Economy
**Decision:** We include NACE sectors **Q** (Health/Social Work) and **S94** (Membership Orgs) from Eurostat.
**Reason:** These are the primary containers for Third Sector activity in European accounts, though they may include some public/private for-profit entities.

### 3. Geographical Mapping (NUTS 2021)
**Decision:** All ISTAT codes are mapped to the **NUTS 2021** standard.
**Reason:** To ensure compatibility with the latest Eurostat shapefiles for PowerBI maps.