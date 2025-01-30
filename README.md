# Data Warehouse de Accidentes de Tráfico

## Descripción del dataset seleccionado
Este conjunto de datos contiene información detallada sobre los conductores de vehículos involucrados en colisiones de tránsito en las carreteras locales y dentro del condado de Montgomery, Maryland. Estos datos son fueron obtenidos a través del Sistema de Informe Automatizado de Accidentes (ACRS) de la Policía Estatal de Maryland y son reportados por diversas agencias policiales, incluyendo la Policía del Condado de Montgomery, la Policía de Gaithersburg, la Policía de Rockville y la Policía de Maryland-National Capital Park.

El conjunto de datos documenta cada colisión registrada en estas áreas, proporcionando detalles sobre los incidentes, los conductores involucrados y otros factores relevantes. Sin embargo, es importante tener en cuenta que la información contenida en estos registros proviene de reportes preliminares entregados por las partes involucradas y los agentes que cubrieron la escena.

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
docker-compose up airflow-init
```

3. 
```bash
docker-compose up -d
```

```bash
docker-compose down -v
```


# Desarrolladores

* **Anthony Montero** - [AnthonyHMR](https://github.com/AnthonyHMR)
* **Daniel Rayo** - [DARD172002](https://github.com/DARD172002)
* **Kun Zheng** - [kunZhen](https://github.com/kunZhen)
