# Data Warehouse de Accidentes de Tráfico

Sistema de análisis de accidentes de tráfico utilizando un modelo dimensional con dos tablas de hechos principales.

## 📊 Modelo de Datos

### Tablas de Hechos

#### 1. FactCrash
* **Métricas:**
  * Número de vehículos involucrados
  * Número de lesiones
  * Número de fatalidades
  * Número de reporte
* **Dimensiones:**
  * Tiempo (DimDateTime_Crash)
  * Ubicación (DimLocation_Crash)
  * Condiciones (DimCondition_Crash)
  * Tipo de Accidente (DimCrashType)

#### 2. FactVehicleInvolvement
* **Métricas:**
  * Severidad de lesiones
  * Indicador de culpabilidad del conductor
  * Circunstancias
* **Dimensiones:**
  * Tiempo (DimDateTime_Veh)
  * Ubicación (DimLocation_Veh)
  * Conductor (DimDriver)
  * Vehículo (DimVehicle)

# Ejecución

1. Ejecutar Docker Desktop

2. 
```bash
docker compose up airflow-init
```

3. 
```bash
docker compose up -d
```


# Desarrolladores

* **Anthony Montero** - [AnthonyHMR](https://github.com/AnthonyHMR)
* **Daniel Rayo** - [DARD172002](https://github.com/DARD172002)
* **Kun Zheng** - [kunZhen](https://github.com/kunZhen)
