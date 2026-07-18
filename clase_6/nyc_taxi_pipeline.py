"""
================================================================
NYC TAXI — PIPELINE LAKEFLOW DECLARATIVE (Arquitectura Medallion)
================================================================

Este archivo define un pipeline ETL declarativo usando
Lakeflow Spark Declarative Pipelines (SDP).

A diferencia de un notebook tradicional:
  - NO se ejecuta interactivamente (no uses display(), no uses .count())
  - NO usamos saveAsTable: el decorador @dp.table crea la tabla
  - Las dependencias entre tablas se DETECTAN automáticamente
  - El pipeline genera un DAG visual: Bronze -> Silver -> Gold

INSTRUCCIONES DE USO:
  1. En Databricks: New -> ETL Pipeline
  2. Elegí catalog = workspace, schema = taxi_lab
  3. En la carpeta 'transformations' pegá este código
  4. Click en "Start" arriba a la derecha
  5. Mirá el DAG generarse en vivo
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# =============================================================
# 🥉 BRONZE — Datos crudos, sin transformar
# =============================================================
# La capa Bronze solo lee la fuente y la persiste tal cual.
# Es nuestra "fuente de verdad" inmutable.

@dp.table(
    name="bronze_trips",
    comment="Viajes de taxi NYC tal como vienen de la fuente (sin transformar)."
)
def bronze_trips():
    return spark.read.table("samples.nyctaxi.trips")


# =============================================================
# 🥈 SILVER — Limpieza + enriquecimiento + DATA QUALITY
# =============================================================
# Acá aparece una FUNCIONALIDAD CLAVE del pipeline declarativo:
# los "Expectations" (@dp.expect_or_drop). Son reglas de calidad
# que el framework aplica y MUESTRA EN EL DASHBOARD.
#
# Tipos de expectation:
#   @dp.expect()         -> registra pero NO descarta filas malas
#   @dp.expect_or_drop() -> descarta filas que no cumplen
#   @dp.expect_or_fail() -> falla el pipeline si alguna no cumple

@dp.table(
    name="silver_trips",
    comment="Viajes limpios con features temporales y de negocio."
)
@dp.expect_or_drop("tarifa_positiva",       "fare_amount > 0")
@dp.expect_or_drop("tarifa_razonable",      "fare_amount <= 500")
@dp.expect_or_drop("distancia_positiva",    "trip_distance > 0")
@dp.expect_or_drop("distancia_razonable",   "trip_distance <= 100")
@dp.expect_or_drop("dropoff_despues_pickup","tpep_dropoff_datetime > tpep_pickup_datetime")
@dp.expect_or_drop("zips_no_nulos",         "pickup_zip IS NOT NULL AND dropoff_zip IS NOT NULL")
def silver_trips():
    return (
        spark.read.table("bronze_trips")
        # ---- Features temporales ----
        .withColumn("pickup_hour",      F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("pickup_day",       F.dayofmonth("tpep_pickup_datetime"))
        .withColumn("pickup_month",     F.month("tpep_pickup_datetime"))
        .withColumn("pickup_year",      F.year("tpep_pickup_datetime"))
        # ---- Duración del viaje ----
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 60
        )
        # ---- Flags de negocio ----
        .withColumn("is_weekend",
                    F.col("pickup_dayofweek").isin([1, 7]).cast("int"))
        .withColumn(
            "time_of_day",
            F.when((F.col("pickup_hour") >= 6)  & (F.col("pickup_hour") < 12), "morning")
             .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") < 18), "afternoon")
             .when((F.col("pickup_hour") >= 18) & (F.col("pickup_hour") < 22), "evening")
             .otherwise("night")
        )
        # ---- Velocidad promedio (para detectar outliers de movimiento) ----
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("trip_duration_min") > 0,
                F.col("trip_distance") / (F.col("trip_duration_min") / 60)
            ).otherwise(None)
        )
        # ---- Filtros adicionales: duración y velocidad realistas ----
        .filter(F.col("trip_duration_min").between(1, 180))
        .filter(F.col("avg_speed_mph").between(1, 80))
    )


# =============================================================
# 🥇 GOLD — Tablas analíticas listas para consumo
# =============================================================
# Las tablas Gold responden a PREGUNTAS DE NEGOCIO concretas.
# Pueden ser varias, cada una con un caso de uso distinto.

@dp.table(
    name="gold_zone_metrics",
    comment="KPIs por zona de origen y franja horaria. Caso de uso: dashboards operativos."
)
def gold_zone_metrics():
    return (
        spark.read.table("silver_trips")
        .groupBy("pickup_zip", "time_of_day")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_mi"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration_min"),
            F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
            F.round(F.avg("avg_speed_mph"), 1).alias("avg_speed"),
        )
    )


@dp.table(
    name="gold_hourly_demand",
    comment="Demanda por hora y día. Caso de uso: planificación de flota / horarios pico."
)
def gold_hourly_demand():
    return (
        spark.read.table("silver_trips")
        .groupBy("pickup_hour", "is_weekend")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_duration_min"), 1).alias("avg_duration"),
        )
        .orderBy("pickup_hour", "is_weekend")
    )


@dp.table(
    name="gold_ml_features",
    comment="Dataset de features listo para entrenamiento de modelos ML."
)
def gold_ml_features():
    # Seleccionamos solo las columnas que necesita el modelo
    return (
        spark.read.table("silver_trips")
        .select(
            "trip_distance",
            "trip_duration_min",
            "pickup_hour",
            "pickup_dayofweek",
            "is_weekend",
            "time_of_day",
            "avg_speed_mph",
            "pickup_zip",
            "fare_amount",  # target variable
        )
    )
