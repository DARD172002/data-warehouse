# Documentación de Consultas SQL para Análisis de Accidentes

## Descripción General

Estas consultas SQL están diseñadas para analizar datos de accidentes en varias dimensiones, proporcionando información valiosa sobre patrones de seguridad vial. Las consultas abordan tres áreas principales:

1. Indicadores Clave de Rendimiento (KPIs) y Totales
2. Métricas de Severidad y Promedios
3. Análisis de Patrones Complejos

### Objetivos Abordados

#### 1. Métricas KPI y Totales
Las consultas se centran en recuentos agregados, incluyendo total de accidentes, clasificación por municipios y distribución de responsabilidad del conductor.

#### 2. Análisis de Severidad
Múltiples consultas analizan la severidad de los accidentes a través de métricas como lesiones promedio y participación de vehículos.

#### 3. Reconocimiento de Patrones
Las consultas contribuyen a identificar patrones no obvios como:
- Tendencias temporales (patrones diarios y mensuales)
- Impacto del clima en la severidad de accidentes
- Correlaciones entre tipo de ruta y tipo de colisión

## Consultas

### 1. Análisis Temporal: Accidentes por Año y Mes

Esta consulta proporciona un análisis de series temporales de la frecuencia de accidentes.

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

**Perspectiva Clave:** Identifica patrones estacionales y tendencias a largo plazo en la frecuencia de accidentes. Esto ayuda a comprender la distribución anual y mensual de accidentes para la asignación de recursos y estrategias de prevención.

### 2. Análisis de Severidad por Día de la Semana

Examina cómo varía la severidad de los accidentes en diferentes días de la semana.

```sql
WITH TotalInjuries AS (
    SELECT SUM(fc.num_injuries) AS total_injuries
    FROM FactCrash AS fc
)
SELECT 
    dt.day_of_week,
    SUM(fc.num_injuries) AS total_injuries_day,
    ROUND((SUM(fc.num_injuries) * 100.0) / (SELECT total_injuries FROM TotalInjuries), 2) AS percentage_injuries
FROM FactCrash AS fc
JOIN DimDateTime_Crash AS dt
    ON fc.date_key_crash = dt.date_key_crash
GROUP BY dt.day_of_week
ORDER BY percentage_injuries DESC;
```

**Perspectiva Clave:** Revela patrones potenciales en la severidad de accidentes relacionados con patrones de tráfico de días laborables versus fines de semana, ayudando a identificar períodos de alto riesgo.

### 3. Análisis de Clima y Tipo de Colisión

Correlaciona las condiciones climáticas con tipos de colisión para entender el impacto ambiental en los accidentes.

```sql
SELECT 
    ct.collision_type, 
    c.weather, 
    COUNT(*) AS accident_count
FROM FactCrash f
JOIN DimCrashType ct 
    ON f.crash_type_key = ct.crash_type_key
JOIN DimCondition_Crash c 
    ON f.condition_key_crash = c.condition_key_crash
GROUP BY ct.collision_type, c.weather
ORDER BY accident_count DESC
LIMIT 10;
```

**Perspectiva Clave:** Ayuda a identificar qué condiciones climáticas están asociadas con tipos específicos de colisiones, permitiendo medidas de seguridad dirigidas durante clima adverso.

### 4. Análisis de Vehículos por Tipo de Colisión

Examina la relación entre tipos de colisión y número de vehículos involucrados.

```sql
SELECT
    ctype.collision_type,
    ROUND(AVG(fc.num_vehicles_involved), 2) AS avg_vehicles_involved
FROM FactCrash AS fc
JOIN DimCrashType AS ctype 
    ON fc.crash_type_key = ctype.crash_type_key
GROUP BY ctype.collision_type
ORDER BY avg_vehicles_involved DESC
LIMIT 25;
```

**Perspectiva Clave:** Revela qué tipos de colisiones típicamente involucran más vehículos, ayudando en la planificación de respuesta a emergencias y asignación de recursos.

### 5. Análisis Anual de Lesiones y Fatalidades

Calcula el total de lesiones y fatalidades por año para rastrear tendencias de seguridad.

```sql
SELECT 
    dt.year, 
    SUM(fc.num_injuries) AS total_injuries, 
    SUM(fc.num_fatalities) AS total_fatalities
FROM DimDateTime_Crash dt
JOIN FactCrash fc 
    ON dt.date_key_crash = fc.date_key_crash
GROUP BY dt.year
ORDER BY dt.year;
```

**Perspectiva Clave:** Proporciona un análisis crucial de tendencias año tras año de la severidad de accidentes, ayudando a medir la efectividad de las iniciativas de seguridad e identificar áreas que necesitan mejora.


### 6. Análisis de Vehículos Involucrados en Accidentes
Esta consulta identifica las marcas y modelos de vehículos más frecuentemente involucrados en accidentes, asegurando que solo se incluyan registros con datos válidos.

```sql
SELECT 
    v.vehicle_make,
    v.vehicle_model,
    COUNT(fv.fact_vehicle_id) AS total_accidentes
FROM 
    DimVehicle v
JOIN 
    FactVehicleInvolment fv ON v.vehicle_key = fv.vehicle_key
WHERE v.vehicle_make <> ''
GROUP BY 
    v.vehicle_make, v.vehicle_model
ORDER BY 
    total_accidentes DESC
LIMIT 10;
```

**Perspectiva Clave:** Revela qué marcas y modelos de vehículos tienen mayor presencia en accidentes, proporcionando información valiosa para fabricantes, aseguradoras y reguladores. Puede ayudar a identificar patrones de seguridad vehicular y evaluar si ciertos modelos tienen un mayor riesgo de colisión.