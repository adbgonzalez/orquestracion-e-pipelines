# Laboratorio: Pipeline de exemplo con Airflow, Kafka, Spark e Lakehouse

## Idea do laboratorio

Neste laboratorio constrúese un pipeline completo e automatizado con:

- `Airflow` como orquestrador
- `Kafka` como capa de entrada de eventos
- `Spark Structured Streaming` para consumir desde Kafka
- `Delta Lake` como formato lakehouse nas capas `bronze`, `silver` e `gold`
- `Postgres` como capa final de serving para consultar os agregados de `gold`

O obxectivo é ter un exemplo docente pequeno, pero completo de extremo a extremo. O DAG de referencia é:

- `pipeline_api_kafka_silver_simple.py`
- `dag_id`: `laboratorio_api_kafka_silver_simple`

## Caso de uso

O pipeline traballa con datos ambientais para varias cidades:

- meteoroloxía diaria desde `Open-Meteo Archive API`
- calidade do aire horaria desde `Open-Meteo Air Quality API`

Airflow chama dúas tarefas produtoras que descargan os datos das APIs e publican mensaxes en Kafka. Despois, Spark consume esas mensaxes e constrúe unha arquitectura Medallion:

- `bronze`: eventos crus persistidos en Delta
- `silver`: datos limpos e normalizados
- `gold`: táboas agregadas orientadas a análise
- `serving`: exportación das táboas `gold` a Postgres

## Fluxo do DAG

```text
produce_weather
  -> kafka_to_bronze_weather
  -> bronze_to_silver_weather_clean

produce_air_quality
  -> kafka_to_bronze_air_quality
  -> bronze_to_silver_air_quality_clean

[bronze_to_silver_weather_clean, bronze_to_silver_air_quality_clean]
  -> silver_join_environment
  -> [silver_to_gold_city_summary,
      silver_to_gold_daily_overview,
      silver_to_gold_pollution_alerts]

silver_to_gold_city_summary
  -> gold_city_summary_to_postgres

silver_to_gold_daily_overview
  -> gold_daily_overview_to_postgres

silver_to_gold_pollution_alerts
  -> gold_pollution_alerts_to_postgres
```

## Rutas de datos

O DAG escribe os datos en HDFS baixo `/user/airflow`:

```text
hdfs://namenode:9000/user/airflow/bronze/weather
hdfs://namenode:9000/user/airflow/bronze/air_quality

hdfs://namenode:9000/user/airflow/silver/weather_clean
hdfs://namenode:9000/user/airflow/silver/air_quality_clean
hdfs://namenode:9000/user/airflow/silver/environment_joined

hdfs://namenode:9000/user/airflow/gold/city_summary
hdfs://namenode:9000/user/airflow/gold/daily_overview
hdfs://namenode:9000/user/airflow/gold/pollution_alerts

hdfs://namenode:9000/user/airflow/checkpoints/weather
hdfs://namenode:9000/user/airflow/checkpoints/air_quality
```

As táboas finais expórtanse a Postgres:

```text
public.city_summary
public.daily_overview
public.pollution_alerts
```

## Estrutura de ficheiros

```text
dags/
  pipeline_api_kafka_silver_simple.py
  data/
    input/
      cities.csv
  src/
    __init__.py
    ingest/
      __init__.py
      kafka_weather_producer.py
      kafka_air_quality_producer.py
    spark_jobs/
      __init__.py
      kafka_to_bronze_weather.py
      kafka_to_bronze_air_quality.py
      bronze_to_silver_weather_clean.py
      bronze_to_silver_air_quality_clean.py
      silver_join_weather_air_quality.py
      silver_to_gold_city_summary.py
      silver_to_gold_daily_overview.py
      silver_to_gold_pollution_alerts.py
      delta_to_postgres.py
```

Os ficheiros `__init__.py` poden estar baleiros. Serven para que Python trate os directorios como paquetes importables desde o DAG.

## Conexións e servizos esperados

O DAG asume que existen estas conexións en Airflow:

- `kafka_default`: conexión co broker Kafka
- `spark_test_ui`: conexión usada polo `SparkSubmitOperator`

Tamén asume estes nomes de servizo dentro da rede de Docker Compose:

- Kafka: `kafka:9092`
- HDFS NameNode: `namenode:9000`
- Postgres serving: `postgres-serving:5432`

Postgres debe ter:

- base de datos: `gold_serving`
- usuario: `gold`
- contrasinal: `gold`

## Código do DAG

Ficheiro: `dags/pipeline_api_kafka_silver_simple.py`

```python
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from src.ingest.kafka_air_quality_producer import producir_mensaxes_air_quality
from src.ingest.kafka_weather_producer import producir_mensaxes_weather


DAGS_DIR = Path(__file__).resolve().parent
HDFS_BASE = "hdfs://namenode:9000/user/airflow"
BRONZE_BASE = f"{HDFS_BASE}/bronze"
SILVER_BASE = f"{HDFS_BASE}/silver"
GOLD_BASE = f"{HDFS_BASE}/gold"
CHECKPOINT_BASE = f"{HDFS_BASE}/checkpoints"
POSTGRES_JDBC_URL = "jdbc:postgresql://postgres-serving:5432/gold_serving"

SPARK_ENV_VARS = {
    "HADOOP_CONF_DIR": "/opt/hadoop-conf",
    "SPARK_CONF_DIR": "/opt/spark/conf",
    "PYSPARK_PYTHON": "python3",
    "PYSPARK_DRIVER_PYTHON": "python3",
}


with DAG(
    dag_id="laboratorio_api_kafka_silver_simple",
    description="Pipeline simple con limpeza en silver e union de weather + air_quality.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["laboratorio", "kafka", "spark", "delta", "silver"],
) as dag:
    produce_weather = ProduceToTopicOperator(
        task_id="produce_weather",
        topic="weather_raw",
        producer_function=producir_mensaxes_weather,
        producer_function_kwargs={
            "cities_path": str(DAGS_DIR / "data" / "input" / "cities.csv"),
        },
        kafka_config_id="kafka_default",
    )

    produce_air_quality = ProduceToTopicOperator(
        task_id="produce_air_quality",
        topic="air_quality_raw",
        producer_function=producir_mensaxes_air_quality,
        producer_function_kwargs={
            "cities_path": str(DAGS_DIR / "data" / "input" / "cities.csv"),
        },
        kafka_config_id="kafka_default",
    )

    kafka_to_bronze_weather = SparkSubmitOperator(
        task_id="kafka_to_bronze_weather",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "kafka_to_bronze_weather.py"),
        application_args=[
            "--topic",
            "weather_raw",
            "--bronze-path",
            f"{BRONZE_BASE}/weather",
            "--checkpoint-path",
            f"{CHECKPOINT_BASE}/weather",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="kafka-to-bronze-weather",
        env_vars=SPARK_ENV_VARS,
    )

    kafka_to_bronze_air_quality = SparkSubmitOperator(
        task_id="kafka_to_bronze_air_quality",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "kafka_to_bronze_air_quality.py"),
        application_args=[
            "--topic",
            "air_quality_raw",
            "--bronze-path",
            f"{BRONZE_BASE}/air_quality",
            "--checkpoint-path",
            f"{CHECKPOINT_BASE}/air_quality",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="kafka-to-bronze-air-quality",
        env_vars=SPARK_ENV_VARS,
    )

    bronze_to_silver_weather_clean = SparkSubmitOperator(
        task_id="bronze_to_silver_weather_clean",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "bronze_to_silver_weather_clean.py"),
        application_args=[
            "--bronze-path",
            f"{BRONZE_BASE}/weather",
            "--silver-path",
            f"{SILVER_BASE}/weather_clean",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="bronze-to-silver-weather-clean",
        env_vars=SPARK_ENV_VARS,
    )

    bronze_to_silver_air_quality_clean = SparkSubmitOperator(
        task_id="bronze_to_silver_air_quality_clean",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "bronze_to_silver_air_quality_clean.py"),
        application_args=[
            "--bronze-path",
            f"{BRONZE_BASE}/air_quality",
            "--silver-path",
            f"{SILVER_BASE}/air_quality_clean",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="bronze-to-silver-air-quality-clean",
        env_vars=SPARK_ENV_VARS,
    )

    silver_join_environment = SparkSubmitOperator(
        task_id="silver_join_environment",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "silver_join_weather_air_quality.py"),
        application_args=[
            "--weather-silver-path",
            f"{SILVER_BASE}/weather_clean",
            "--air-quality-silver-path",
            f"{SILVER_BASE}/air_quality_clean",
            "--silver-path",
            f"{SILVER_BASE}/environment_joined",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="silver-join-environment",
        env_vars=SPARK_ENV_VARS,
    )

    silver_to_gold_city_summary = SparkSubmitOperator(
        task_id="silver_to_gold_city_summary",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "silver_to_gold_city_summary.py"),
        application_args=[
            "--silver-path",
            f"{SILVER_BASE}/environment_joined",
            "--gold-path",
            f"{GOLD_BASE}/city_summary",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="silver-to-gold-city-summary",
        env_vars=SPARK_ENV_VARS,
    )

    silver_to_gold_daily_overview = SparkSubmitOperator(
        task_id="silver_to_gold_daily_overview",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "silver_to_gold_daily_overview.py"),
        application_args=[
            "--silver-path",
            f"{SILVER_BASE}/environment_joined",
            "--gold-path",
            f"{GOLD_BASE}/daily_overview",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="silver-to-gold-daily-overview",
        env_vars=SPARK_ENV_VARS,
    )

    silver_to_gold_pollution_alerts = SparkSubmitOperator(
        task_id="silver_to_gold_pollution_alerts",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "silver_to_gold_pollution_alerts.py"),
        application_args=[
            "--silver-path",
            f"{SILVER_BASE}/environment_joined",
            "--gold-path",
            f"{GOLD_BASE}/pollution_alerts",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="silver-to-gold-pollution-alerts",
        env_vars=SPARK_ENV_VARS,
    )

    gold_city_summary_to_postgres = SparkSubmitOperator(
        task_id="gold_city_summary_to_postgres",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "delta_to_postgres.py"),
        application_args=[
            "--input-path",
            f"{GOLD_BASE}/city_summary",
            "--jdbc-url",
            POSTGRES_JDBC_URL,
            "--dbtable",
            "public.city_summary",
            "--db-user",
            "gold",
            "--db-password",
            "gold",
            "--write-mode",
            "overwrite",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="gold-city-summary-to-postgres",
        packages="org.postgresql:postgresql:42.7.7",
        env_vars=SPARK_ENV_VARS,
    )

    gold_daily_overview_to_postgres = SparkSubmitOperator(
        task_id="gold_daily_overview_to_postgres",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "delta_to_postgres.py"),
        application_args=[
            "--input-path",
            f"{GOLD_BASE}/daily_overview",
            "--jdbc-url",
            POSTGRES_JDBC_URL,
            "--dbtable",
            "public.daily_overview",
            "--db-user",
            "gold",
            "--db-password",
            "gold",
            "--write-mode",
            "overwrite",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="gold-daily-overview-to-postgres",
        packages="org.postgresql:postgresql:42.7.7",
        env_vars=SPARK_ENV_VARS,
    )

    gold_pollution_alerts_to_postgres = SparkSubmitOperator(
        task_id="gold_pollution_alerts_to_postgres",
        application=str(DAGS_DIR / "src" / "spark_jobs" / "delta_to_postgres.py"),
        application_args=[
            "--input-path",
            f"{GOLD_BASE}/pollution_alerts",
            "--jdbc-url",
            POSTGRES_JDBC_URL,
            "--dbtable",
            "public.pollution_alerts",
            "--db-user",
            "gold",
            "--db-password",
            "gold",
            "--write-mode",
            "overwrite",
        ],
        conn_id="spark_test_ui",
        spark_binary="/home/airflow/.local/bin/spark-submit",
        name="gold-pollution-alerts-to-postgres",
        packages="org.postgresql:postgresql:42.7.7",
        env_vars=SPARK_ENV_VARS,
    )

    produce_weather >> kafka_to_bronze_weather >> bronze_to_silver_weather_clean
    produce_air_quality >> kafka_to_bronze_air_quality >> bronze_to_silver_air_quality_clean
    [bronze_to_silver_weather_clean, bronze_to_silver_air_quality_clean] >> silver_join_environment
    silver_join_environment >> [
        silver_to_gold_city_summary,
        silver_to_gold_daily_overview,
        silver_to_gold_pollution_alerts,
    ]
    silver_to_gold_city_summary >> gold_city_summary_to_postgres
    silver_to_gold_daily_overview >> gold_daily_overview_to_postgres
    silver_to_gold_pollution_alerts >> gold_pollution_alerts_to_postgres
```

## Ficheiro de cidades

Ficheiro: `dags/data/input/cities.csv`

```csv
city,country,latitude,longitude
Santiago de Compostela,ES,42.8782,-8.5448
A Coruna,ES,43.3623,-8.4115
Vigo,ES,42.2406,-8.7207
Madrid,ES,40.4168,-3.7038
Barcelona,ES,41.3874,2.1686
```

## Produtores Kafka

### Meteoroloxía

Ficheiro: `dags/src/ingest/kafka_weather_producer.py`

```python
import csv
import json
from datetime import datetime, timezone

import requests


def load_cities(cities_path: str) -> list[dict]:
    with open(cities_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fetch_weather(city: dict) -> dict:
    response = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "start_date": "2025-01-01",
            "end_date": "2025-01-07",
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": "auto",
        },
        timeout=60,
    )
    response.raise_for_status()
    return {
        "city": city["city"],
        "country": city["country"],
        "latitude": float(city["latitude"]),
        "longitude": float(city["longitude"]),
        "ingestion_ts": datetime.now(timezone.utc).isoformat(),
        "payload": response.json(),
    }


def producir_mensaxes_weather(cities_path: str) -> list[tuple[str, str]]:
    mensaxes = []

    for city in load_cities(cities_path):
        payload = fetch_weather(city)
        mensaxes.append((city["city"], json.dumps(payload)))

    return mensaxes
```

### Calidade do aire

Ficheiro: `dags/src/ingest/kafka_air_quality_producer.py`

```python
import csv
import json
from datetime import datetime, timezone

import requests


def load_cities(cities_path: str) -> list[dict]:
    with open(cities_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fetch_air_quality(city: dict) -> dict:
    response = requests.get(
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        params={
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "hourly": "pm10,pm2_5,nitrogen_dioxide,ozone,european_aqi",
            "start_date": "2025-01-01",
            "end_date": "2025-01-07",
            "timezone": "auto",
        },
        timeout=60,
    )
    response.raise_for_status()
    return {
        "city": city["city"],
        "country": city["country"],
        "latitude": float(city["latitude"]),
        "longitude": float(city["longitude"]),
        "ingestion_ts": datetime.now(timezone.utc).isoformat(),
        "payload": response.json(),
    }


def producir_mensaxes_air_quality(cities_path: str) -> list[tuple[str, str]]:
    mensaxes = []

    for city in load_cities(cities_path):
        payload = fetch_air_quality(city)
        mensaxes.append((city["city"], json.dumps(payload)))

    return mensaxes
```

## Jobs Spark: Kafka a Bronze

### Weather raw a Bronze Delta

Ficheiro: `dags/src/spark_jobs/kafka_to_bronze_weather.py`

```python
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="weather_raw")
    parser.add_argument("--bronze-path", default="data/lakehouse/bronze/weather")
    parser.add_argument("--checkpoint-path", default="data/checkpoints/weather")
    args = parser.parse_args()

    daily_schema = StructType(
        [
            StructField("time", ArrayType(StringType()), True),
            StructField("temperature_2m_max", ArrayType(DoubleType()), True),
            StructField("temperature_2m_min", ArrayType(DoubleType()), True),
            StructField("precipitation_sum", ArrayType(DoubleType()), True),
        ]
    )

    payload_schema = StructType([StructField("daily", daily_schema, True)])

    schema = StructType(
        [
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("ingestion_ts", StringType(), True),
            StructField("payload", payload_schema, True),
        ]
    )

    spark = SparkSession.builder.appName("kafka_to_bronze_weather").getOrCreate()

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    query = (
        parsed_df.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start(args.bronze_path)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
```

### Air quality raw a Bronze Delta

Ficheiro: `dags/src/spark_jobs/kafka_to_bronze_air_quality.py`

```python
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="air_quality_raw")
    parser.add_argument("--bronze-path", default="data/lakehouse/bronze/air_quality")
    parser.add_argument("--checkpoint-path", default="data/checkpoints/air_quality")
    args = parser.parse_args()

    schema = StructType(
        [
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("ingestion_ts", StringType(), True),
            StructField("payload", StructType([StructField("results", StringType(), True)]), True),
        ]
    )

    spark = SparkSession.builder.appName("kafka_to_bronze_air_quality").getOrCreate()

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze_df = raw_df.select(
        col("value").cast("string").alias("raw_json"),
        col("timestamp").alias("kafka_ts"),
        from_json(col("value").cast("string"), schema).alias("data"),
    ).select("raw_json", "kafka_ts", "data.*")

    query = (
        bronze_df.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start(args.bronze_path)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
```

## Jobs Spark: Bronze a Silver

### Limpeza de meteoroloxía

Ficheiro: `dags/src/spark_jobs/bronze_to_silver_weather_clean.py`

```python
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, col, explode_outer, initcap, to_date, to_timestamp, trim


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", default="data/lakehouse/bronze/weather")
    parser.add_argument("--silver-path", default="data/lakehouse/silver/weather_clean")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("bronze_to_silver_weather_clean").getOrCreate()

    bronze_df = spark.read.format("delta").load(args.bronze_path)

    silver_df = (
        bronze_df.select(
            initcap(trim(col("city"))).alias("city"),
            initcap(trim(col("country"))).alias("country"),
            to_timestamp("ingestion_ts").alias("ingestion_ts"),
            explode_outer(
                arrays_zip(
                    col("payload.daily.time"),
                    col("payload.daily.temperature_2m_max"),
                    col("payload.daily.temperature_2m_min"),
                    col("payload.daily.precipitation_sum"),
                )
            ).alias("daily_row"),
        )
        .select(
            "city",
            "country",
            "ingestion_ts",
            to_date(col("daily_row.time")).alias("event_date"),
            col("daily_row.temperature_2m_max").cast("double").alias("temp_max"),
            col("daily_row.temperature_2m_min").cast("double").alias("temp_min"),
            col("daily_row.precipitation_sum").cast("double").alias("precipitation"),
        )
        .dropna(subset=["city", "country", "event_date"])
        .dropDuplicates(["city", "country", "event_date"])
    )

    silver_df.write.format("delta").mode("overwrite").save(args.silver_path)


if __name__ == "__main__":
    main()
```

### Limpeza de calidade do aire

Ficheiro: `dags/src/spark_jobs/bronze_to_silver_air_quality_clean.py`

```python
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, avg, col, explode_outer, from_json, initcap, to_date, to_timestamp, trim
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", default="data/lakehouse/bronze/air_quality")
    parser.add_argument("--silver-path", default="data/lakehouse/silver/air_quality_clean")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("bronze_to_silver_air_quality_clean").getOrCreate()

    payload_schema = StructType(
        [
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("ingestion_ts", StringType(), True),
            StructField(
                "payload",
                StructType(
                    [
                        StructField(
                            "hourly",
                            StructType(
                                [
                                    StructField("time", ArrayType(StringType()), True),
                                    StructField("pm10", ArrayType(DoubleType()), True),
                                    StructField("pm2_5", ArrayType(DoubleType()), True),
                                    StructField("nitrogen_dioxide", ArrayType(DoubleType()), True),
                                    StructField("ozone", ArrayType(DoubleType()), True),
                                ]
                            ),
                            True,
                        )
                    ]
                ),
                True,
            ),
        ]
    )

    bronze_df = spark.read.format("delta").load(args.bronze_path)

    parsed_df = bronze_df.select(from_json(col("raw_json"), payload_schema).alias("data")).select("data.*")

    hourly_df = (
        parsed_df.select(
            initcap(trim(col("city"))).alias("city"),
            initcap(trim(col("country"))).alias("country"),
            to_timestamp("ingestion_ts").alias("ingestion_ts"),
            explode_outer(
                arrays_zip(
                    col("payload.hourly.time"),
                    col("payload.hourly.pm10"),
                    col("payload.hourly.pm2_5"),
                    col("payload.hourly.nitrogen_dioxide"),
                    col("payload.hourly.ozone"),
                )
            ).alias("hourly_row"),
        )
        .select(
            "city",
            "country",
            "ingestion_ts",
            to_date(col("hourly_row.time")).alias("event_date"),
            col("hourly_row.pm10").cast("double").alias("pm10"),
            col("hourly_row.pm2_5").cast("double").alias("pm25"),
            col("hourly_row.nitrogen_dioxide").cast("double").alias("no2"),
            col("hourly_row.ozone").cast("double").alias("o3"),
        )
        .dropna(subset=["city", "country", "event_date"])
    )

    silver_df = hourly_df.groupBy("city", "country", "event_date").agg(
        avg("pm10").alias("pm10"),
        avg("pm25").alias("pm25"),
        avg("no2").alias("no2"),
        avg("o3").alias("o3"),
    )

    silver_df.write.format("delta").mode("overwrite").save(args.silver_path)


if __name__ == "__main__":
    main()
```

### Unión das dúas fontes en Silver

Ficheiro: `dags/src/spark_jobs/silver_join_weather_air_quality.py`

```python
import argparse

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--weather-silver-path", default="data/lakehouse/silver/weather_clean")
    parser.add_argument("--air-quality-silver-path", default="data/lakehouse/silver/air_quality_clean")
    parser.add_argument("--silver-path", default="data/lakehouse/silver/environment_joined")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("silver_join_weather_air_quality").getOrCreate()

    weather_df = spark.read.format("delta").load(args.weather_silver_path)
    air_quality_df = spark.read.format("delta").load(args.air_quality_silver_path)

    joined_df = weather_df.join(
        air_quality_df,
        on=["city", "country", "event_date"],
        how="inner",
    )

    joined_df.write.format("delta").mode("overwrite").save(args.silver_path)


if __name__ == "__main__":
    main()
```

## Jobs Spark: Silver a Gold

### Resumo por cidade

Ficheiro: `dags/src/spark_jobs/silver_to_gold_city_summary.py`

```python
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as spark_sum


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", default="data/lakehouse/silver/environment_joined")
    parser.add_argument("--gold-path", default="data/lakehouse/gold/city_summary")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("silver_to_gold_city_summary").getOrCreate()

    silver_df = spark.read.format("delta").load(args.silver_path)

    gold_df = silver_df.groupBy("city", "country").agg(
        avg("temp_max").alias("avg_temp_max"),
        avg("temp_min").alias("avg_temp_min"),
        spark_sum("precipitation").alias("total_precipitation"),
        avg("pm25").alias("avg_pm25"),
        avg("pm10").alias("avg_pm10"),
    )

    gold_df.write.format("delta").mode("overwrite").save(args.gold_path)


if __name__ == "__main__":
    main()
```

### Vista diaria global

Ficheiro: `dags/src/spark_jobs/silver_to_gold_daily_overview.py`

```python
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", default="data/lakehouse/silver/environment_joined")
    parser.add_argument("--gold-path", default="data/lakehouse/gold/daily_overview")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("silver_to_gold_daily_overview").getOrCreate()

    silver_df = spark.read.format("delta").load(args.silver_path)

    gold_df = silver_df.groupBy("event_date").agg(
        avg("temp_max").alias("avg_temp_max"),
        avg("precipitation").alias("avg_precipitation"),
        avg("pm25").alias("avg_pm25"),
        avg("no2").alias("avg_no2"),
    )

    gold_df.write.format("delta").mode("overwrite").save(args.gold_path)


if __name__ == "__main__":
    main()
```

### Alertas de contaminación

Ficheiro: `dags/src/spark_jobs/silver_to_gold_pollution_alerts.py`

```python
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-path", default="data/lakehouse/silver/environment_joined")
    parser.add_argument("--gold-path", default="data/lakehouse/gold/pollution_alerts")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("silver_to_gold_pollution_alerts").getOrCreate()

    silver_df = spark.read.format("delta").load(args.silver_path)

    gold_df = silver_df.filter((col("pm25") > 15) | (col("no2") > 40)).select(
        "city",
        "country",
        "event_date",
        "pm25",
        "no2",
        "temp_max",
        "precipitation",
    )

    gold_df.write.format("delta").mode("overwrite").save(args.gold_path)


if __name__ == "__main__":
    main()
```

## Exportación de Gold a Postgres

Ficheiro: `dags/src/spark_jobs/delta_to_postgres.py`

```python
import argparse

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--jdbc-url", default="jdbc:postgresql://postgres-serving:5432/gold_serving")
    parser.add_argument("--dbtable", required=True)
    parser.add_argument("--db-user", default="gold")
    parser.add_argument("--db-password", default="gold")
    parser.add_argument("--write-mode", default="overwrite")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("delta_to_postgres").getOrCreate()

    df = spark.read.format("delta").load(args.input_path)

    (
        df.write.format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", args.dbtable)
        .option("user", args.db_user)
        .option("password", args.db_password)
        .option("driver", "org.postgresql.Driver")
        .mode(args.write_mode)
        .save()
    )


if __name__ == "__main__":
    main()
```

## Que demostra este exemplo

Este pipeline cobre os elementos principais dun proxecto lakehouse con orquestración:

- Airflow non só lanza scripts: define dependencias reais entre tarefas.
- Kafka actúa como punto de entrada dos eventos crus.
- Spark Structured Streaming materializa os datos de Kafka en `bronze`.
- Spark batch limpa, normaliza, une e agrega os datos.
- Delta é o formato usado nas capas `bronze`, `silver` e `gold`.
- Postgres funciona como capa final de serving para ferramentas externas.

O deseño é pequeno de propósito, pero permite explicar con claridade a diferenza entre inxesta, persistencia lakehouse, transformación, agregación e consumo final.
