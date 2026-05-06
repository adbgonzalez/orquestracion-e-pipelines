# Traballo final: pipeline de datos con Airflow, Spark e Lakehouse

## Idea xeral

Este traballo final substitúe a proba escrita desta parte da materia. A súa finalidade é que cada alumno ou alumna deseñe e implemente un pipeline de datos completo empregando as tecnoloxías centrais vistas no curso.

O tema é libre, sempre que se cumpran os requisitos técnicos definidos neste enunciado.

O obxectivo non é construír o proxecto máis complexo posible, senón demostrar que se sabe deseñar e automatizar un fluxo de datos real de extremo a extremo.

---

## Obxectivo

Deseñar e implementar un pipeline de datos automatizado mediante un DAG de Airflow que:

* inxira un ou varios conxuntos de datos
* execute transformacións con Spark
* persista os resultados en formato lakehouse (Delta ou Iceberg)
* dispoña dunha saída final utilizable (consumo)

---

## Requisitos obrigatorios

O traballo deberá incluír obrigatoriamente:

* un DAG funcional en Airflow
* uso real de Spark para transformacións
* almacenamento en Delta ou Iceberg
* execución automatizada
* unha estrutura por capas
* documentación básica

---

## Arquitectura do pipeline

Recoméndase o uso dunha arquitectura tipo Medallion (`bronze`, `silver`, `gold`).
Tamén se aceptarán outras organizacións por capas sempre que estean ben xustificadas.

---

## Ferramentas

### Obrigatorias

* Airflow
* Spark
* Delta ou Iceberg

### Opcionais

Poderán empregarse ferramentas adicionais sempre que non substitúan as obrigatorias.

Exemplos:

* Kafka, NiFi ou similares para inxesta
* integración de varias fontes
* exportación a bases de datos
* visualización de datos

---

## Tema e fontes de datos

O tema é libre.

As fontes de datos deben permitir construír un pipeline realista. Recoméndase empregar datasets públicos ou APIs abertas.

Valorarase positivamente o uso de máis dunha fonte de datos cando teña sentido.

---

## Estrutura orientativa

1. inxesta de datos
2. almacenamento inicial
3. transformación con Spark
4. almacenamento en lakehouse
5. capa de consumo
6. automatización con Airflow

---

## Formato de entrega

### Estrutura do proxecto

A entrega deberá realizarse como **un único repositorio ou arquivo comprimido** coa seguinte estrutura:

```
proyecto/
└── dags/
    ├── dag_principal.py
    ├── spark/
    ├── data/ (opcional)
    ├── utils/ (opcional)
    ├── docker-compose.yml (opcional)
    ├── README.md
    └── memoria.pdf (ou .md)
```

### Consideracións importantes

* Todo o necesario para executar o pipeline debe estar dentro do directorio `dags/`.
* O DAG debe poder executarse directamente desde ese directorio.
* As rutas empregadas no código deben ser coherentes con esta estrutura.
* Non se permitirá depender de rutas externas ao directorio `dags/`, salvo que estea debidamente xustificado.

### Contido mínimo

A entrega debe incluír:

* DAG(s) de Airflow funcionais
* scripts de Spark
* código auxiliar necesario
* README.md coas instrucións
* memoria técnica

### Reproducibilidade

O proxecto debe poder executarse seguindo as instrucións proporcionadas.

Se non é posible reproducir o funcionamento do pipeline, a cualificación poderá verse reducida.

### Datos e evidencia

Debe incluírse polo menos unha das seguintes opcións:

* datos de saída (mostra ou snapshot)
* evidencias da execución:

  * capturas de Airflow
  * resultados xerados
  * logs relevantes


## Memoria técnica

Debe incluír:

* descrición do problema
* fontes de datos
* arquitectura do pipeline
* explicación das capas
* descrición do DAG
* transformacións principais
* decisións técnicas
* dificultades atopadas

---

## Rúbrica de avaliación (10 puntos)

| Criterio                              | Insuficiente                                       | Básico                                          | Adecuado                                                                                               | Avanzado                                                                                                                                             |
| ------------------------------------- | -------------------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Orquestración (Airflow)** (1.5)  | DAG inexistente ou non funcional (0)               | DAG funcional pero simple (0.75)                | Dependencias correctas e execución coherente (1.25)                                                    | DAG ben estruturado, modular e preparado para reexecución (1.5)                                                                                      |
| **2. Fontes e inxesta** (1.5)         | Sen inxesta real (0)                               | Fonte única simple (0.75)                       | Inxesta automatizada e coherente (1.25)                                                                | Múltiples fontes ou uso de ferramentas (Kafka, NiFi...) con sentido (1.5)                                                                            |
| **3. Procesamento (Spark)** (1.5)     | Sen transformación real (0)                        | Transformacións simples (0.75)                  | Limpeza e transformacións correctas (1.25)                                                             | Join entre táboas, agregacións relevantes e lóxica coherente (1.5)                                                                                   |
| **4. Almacenamento e modelado** (1.5) | Uso incorrecto de Delta/Iceberg (0)                | Uso básico (0.75)                               | Organización coherente (1.25)                                                                          | Uso en MinIO/HDFS (non trivial), bo modelo de datos e sen información innecesaria (1.5)                                                              |
| **5. Capa de consumo** (1.5)          | Sen saída clara (0)                                | Datos dispoñibles no lakehouse (0.75)           | Exportación a BD ou sistema externo (1.25)                                                             | Visualización ou saída representativa e útil (1.5)                                                                                                   |
| **6. Memoria e documentación** (2.5)  | Inexistente ou non permite entender o proxecto (0) | Descrición superficial sen xustificación (1.25) | Explica correctamente o pipeline, decisións principais e inclúe capturas das execucións/resultados (2) | Explicación clara e estruturada, xustificación das decisións técnicas, comprensión global e capturas relevantes que evidencian o funcionamento (2.5) |
                       |

---

## Consideracións importantes

* Os elementos avanzados deben estar integrados no pipeline con sentido.
* Non se valorará positivamente o uso de ferramentas ou técnicas sen xustificación.
* A coherencia global do pipeline terá un peso importante na avaliación.

---

## Anexo: Fontes de datos e ferramentas complementarias

### Fontes de datos recomendadas

Para facilitar o desenvolvemento do proxecto, recoméndase empregar fontes de datos públicas e accesibles.

---

### Datasets públicos

* Kaggle → https://www.kaggle.com/datasets
* UCI Machine Learning Repository → https://archive.ics.uci.edu/
* data.gov → https://data.gov/
* data.europa.eu → https://data.europa.eu/
* Google Dataset Search → https://datasetsearch.research.google.com/

---

### APIs públicas (exemplos por temática)

#### Meteoroloxía

* Open-Meteo → https://open-meteo.com/
* OpenWeather → https://openweathermap.org/api
* Weatherbit → https://www.weatherbit.io/api

---

#### Finanzas e criptomoedas

* CoinGecko → https://www.coingecko.com/en/api
* Alpha Vantage → https://www.alphavantage.co/
* Binance API → https://binance-docs.github.io/apidocs/
* Yahoo Finance (non oficial) → https://rapidapi.com/apidojo/api/yahoo-finance1

---

#### Mobilidade e transporte

* CityBikes → http://api.citybik.es/v2/
* Transport for London → https://api.tfl.gov.uk/
* Navitia (transporte público) → https://www.navitia.io/
* OpenChargeMap (vehículos eléctricos) → https://openchargemap.org/site/develop/api

---

#### Datos xeográficos e mapas

* OpenStreetMap / Overpass → https://overpass-api.de/
* Mapbox → https://docs.mapbox.com/api/
* GeoNames → http://www.geonames.org/export/web-services.html

---

#### Saúde

* Disease.sh (COVID e outros) → https://disease.sh/
* WHO API (datos OMS) → https://www.who.int/data
* CDC → https://data.cdc.gov/

---

#### Comercio e economía

* Fake Store API → https://fakestoreapi.com/
* Open Food Facts → https://world.openfoodfacts.org/data
* World Bank API → https://data.worldbank.org/

---

#### Cultura, medios e entretemento

* TMDB (películas) → https://www.themoviedb.org/documentation/api
* Spotify API → https://developer.spotify.com/documentation/web-api/
* YouTube Data API → https://developers.google.com/youtube/v3

---

#### Deportes

* Football-Data → https://www.football-data.org/
* API-Sports → https://www.api-sports.io/
* SportdataAPI → https://sportdataapi.com/

---

#### Datos xerais / múltiples APIs

* RapidAPI → https://rapidapi.com/
* Public APIs → https://publicapis.dev/
* ProgrammableWeb → https://www.programmableweb.com/apis/directory

---

### Ferramentas complementarias

O uso destas ferramentas non é obrigatorio, pero pode achegar valor ao proxecto se se emprega con sentido.

#### Visualización e explotación

* Apache Superset
* Streamlit
* Metabase

#### Inxesta e integración de datos

* Apache Kafka
* Apache NiFi

#### Almacenamento e servizo

* PostgreSQL
* MinIO

#### Outras utilidades

* dbt
* Jupyter notebooks

---

### Consideracións

* Non é necesario empregar moitas fontes ou ferramentas, senón facelo con criterio.
* Valorarase que as fontes estean ben escollidas e integradas.
* O proxecto debe seguir sendo comprensible e reproducible.
