# Crash Analysis SQL Queries Documentation

## Overview

These SQL queries are designed to analyze crash data across several dimensions, providing valuable insights into traffic safety patterns. The queries address three main areas:

1. Key Performance Indicators (KPIs) and Totals
2. Severity Metrics and Averages
3. Complex Pattern Analysis

### Goals Addressed

#### 1. KPIs and Total Metrics
Queries 1, 2, 5, and 7 focus on aggregate counts, including total crashes, municipality rankings, and driver fault distribution.

#### 2. Severity Analysis
Queries 3, 4, and 6 analyze crash severity through metrics like average injuries and vehicle involvement.

#### 3. Pattern Recognition
All queries contribute to identifying non-obvious patterns such as:
- Temporal trends (daily and monthly patterns)
- Weather impact on crash severity
- Route type and collision type correlations

## Queries

### 1. Temporal Analysis: Crashes by Year and Month

This query provides a time-series analysis of crash frequency.

```sql
SELECT 
    dt.year,
    dt.month,
    COUNT(fc.fact_crash_id) AS total_crashes
FROM FactCrash AS fc
JOIN DimDateTime_Crash AS dt 
    ON fc.date_key_crash = dt.date_key_crash
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month;
```

**Key Insight:** Identifies seasonal patterns and long-term trends in crash frequency.

### 2. Geographic Analysis: Top 5 High-Crash Municipalities

Identifies municipalities with the highest crash frequencies.

```sql
SELECT
    loc.municipality,
    COUNT(fc.fact_crash_id) AS total_crashes
FROM FactCrash AS fc
JOIN DimLocation_Crash AS loc 
    ON fc.location_key_crash = loc.location_key_crash
GROUP BY loc.municipality
ORDER BY total_crashes DESC
LIMIT 5;
```

**Key Insight:** Highlights areas requiring focused safety interventions.

### 3. Day-of-Week Severity Analysis

Examines how crash severity varies across different days of the week.

```sql
SELECT 
    dt.day_of_week,
    ROUND(AVG(fc.num_injuries), 2) AS avg_injuries
FROM FactCrash AS fc
JOIN DimDateTime_Crash AS dt
    ON fc.date_key_crash = dt.date_key_crash
GROUP BY dt.day_of_week
ORDER BY avg_injuries DESC;
```

**Key Insight:** Reveals potential patterns in crash severity related to weekday vs. weekend traffic patterns.

### 4. Weather Impact Analysis

Correlates weather conditions with crash severity.

```sql
SELECT
    cond.weather,
    ROUND(AVG(fc.num_injuries), 2) AS avg_injuries
FROM FactCrash AS fc
JOIN DimCondition_Crash AS cond
    ON fc.condition_key_crash = cond.condition_key_crash
GROUP BY cond.weather
ORDER BY avg_injuries DESC
LIMIT 5;
```

**Key Insight:** Quantifies the relationship between weather conditions and crash severity.

### 5. Infrastructure Analysis: Crashes by Route Type

Analyzes crash distribution across different road classifications.

```sql
SELECT
    loc.route_type,
    COUNT(fc.fact_crash_id) AS total_crashes
FROM FactCrash AS fc
JOIN DimLocation_Crash AS loc
    ON fc.location_key_crash = loc.location_key_crash
GROUP BY loc.route_type
ORDER BY total_crashes DESC;
```

**Key Insight:** Identifies which road types experience disproportionate crash volumes.

### 6. Collision Type Vehicle Analysis

Examines the relationship between collision types and number of vehicles involved.

```sql
SELECT
    ctype.collision_type,
    ROUND(AVG(fc.num_vehicles_involved), 2) AS avg_vehicles_involved
FROM FactCrash AS fc
JOIN DimCrashType AS ctype 
    ON fc.crash_type_key = ctype.crash_type_key
GROUP BY ctype.collision_type
ORDER BY avg_vehicles_involved DESC;
```

**Key Insight:** Reveals which collision types typically involve more vehicles.

### 7. Driver Fault Analysis

Analyzes the distribution of fault attribution in crashes.

```sql
SELECT
    drv.driver_at_fault,
    COUNT(fv.fact_vehicle_id) AS total_vehicles
FROM FactVehicleInvolment AS fv
JOIN DimDriver AS drv
    ON fv.driver_key = drv.driver_key
GROUP BY drv.driver_at_fault
ORDER BY total_vehicles DESC;
```

**Key Insight:** Provides insights into fault attribution patterns in crash records.

