# 3. Xestión de datos en Databricks

## 3.1 O modelo de almacenamento en Databricks

Databricks utiliza un modelo de almacenamento baseado en **Delta Lake**, que constitúe a base do enfoque *lakehouse*.

Neste modelo, os datos almacénanse en formato aberto (baseado en Parquet), pero engadindo unha capa adicional que permite:

- transaccións ACID  
- control de versións (*time travel*)  
- control de esquema  
- mellora do rendemento en consultas  

Este enfoque permite combinar as vantaxes dos data lakes (flexibilidade e escalabilidade) coas dos data warehouses (consistencia e eficiencia).

---

## 3.2 Organización dos datos en Databricks

En Databricks, os datos poden almacenarse e organizarse de diferentes formas, dependendo do seu uso dentro da plataforma.

De forma xeral, pódense distinguir dous niveis principais:

- **ficheiros** (nivel físico)
- **táboas** (nivel lóxico)

---

### 3.2.1 Ficheiros

Os ficheiros representan o nivel máis básico de almacenamento.

Poden ser:

- CSV  
- JSON  
- Parquet  
- ficheiros Delta  

Estes ficheiros almacénanse nun sistema distribuído, como:

- DBFS (Databricks File System)  
- almacenamento cloud (S3, ADLS, GCS)  

Desde Databricks, pódese acceder a estes ficheiros mediante rutas.

Exemplo:

```python
df = spark.read.csv("/Volumes/exemplo/esquema/volume/datos.csv", header=True)
df.show()
```

Traballar directamente con ficheiros é útil para:

- importar datos  
- explorar información en bruto  
- realizar procesamentos iniciais  

Con todo, este enfoque ten limitacións en termos de organización, control e rendemento.

---

### 3.2.2 Táboas

As táboas representan un nivel superior de abstracción.

En lugar de traballar con ficheiros directamente, os datos organízanse en estruturas que permiten:

- consultas mediante SQL  
- control de acceso  
- mellor rendemento  
- xestión do esquema  

As táboas en Databricks baséanse habitualmente en **Delta Lake**.

Exemplo:

```sql
SELECT * FROM persoas;
```

---

### 3.2.3 Relación entre ficheiros e táboas

Unha táboa en Databricks non deixa de ser un conxunto de ficheiros almacenados fisicamente.

👉 É dicir:

- os **ficheiros** conteñen os datos reais  
- as **táboas** proporcionan unha forma estruturada de acceder a eses datos  

Esta separación entre nivel físico e lóxico é fundamental para entender o funcionamento do sistema.

---

### 3.2.4 Organización lóxica: catálogo, esquema e táboa

Para organizar os datos, Databricks utiliza unha estrutura xerárquica:

```
catálogo → esquema → táboa
```

Esta organización forma parte de **Unity Catalog**.

---

#### Catálogo

O catálogo é o nivel superior.

Permite:

- separar datos por proxectos ou equipos  
- definir políticas de acceso  

---

#### Esquema

Un esquema (ou base de datos) agrupa táboas relacionadas.

---

#### Táboa

A táboa contén os datos estruturados e é o elemento principal de traballo.

---

### 3.2.5 Tipos de táboas

En Databricks pódense definir diferentes tipos de táboas:

#### Táboas xestionadas (managed)

- Databricks controla os datos  
- os ficheiros almacénanse automaticamente  
- ao eliminar a táboa, elimínanse os datos  

---

#### Táboas externas (external)

- os datos almacénanse fóra do control directo de Databricks  
- a táboa só referencia os ficheiros  
- ao eliminar a táboa, os datos permanecen  

---

### 3.2.6 Resumo conceptual

O modelo de datos en Databricks pódese entender do seguinte modo:

- os datos almacénanse como **ficheiros**  
- organízanse como **táboas**  
- estrutúranse mediante **catálogos e esquemas**  

Este modelo permite combinar flexibilidade (ficheiros) con eficiencia e organización (táboas).
---

## 3.3 Unity Catalog

**Unity Catalog** é o sistema de gobernanza de datos de Databricks.

Permite xestionar de forma centralizada como se organizan e acceden os datos dentro da plataforma.

---

### 3.3.1 Funcións principais

Unity Catalog engade unha capa de control sobre os datos, permitindo:

- definir permisos de acceso a nivel de catálogo, esquema e táboa  
- controlar que usuarios ou grupos poden consultar ou modificar datos  
- centralizar a xestión de datos en diferentes contornos de traballo  

---

### 3.3.2 Control de acceso

Un dos aspectos máis importantes de Unity Catalog é a xestión de permisos.

Permite definir quen pode:

- ler datos  
- modificar táboas  
- crear novas estruturas  

Este control é fundamental en contornos reais, onde diferentes usuarios poden ter distintos niveis de acceso.

---

### 3.3.3 Organización dos datos

Unity Catalog utiliza a estrutura:

```
catálogo → esquema → táboa
```

pero engade a capacidade de:

- separar datos por proxectos ou equipos  
- aplicar políticas de acceso sobre cada nivel  
- manter unha organización consistente dos datos  

---

### 3.3.4 Importancia en contornos reais

En sistemas de datos reais, non só é importante almacenar e procesar información, senón tamén:

- garantir a seguridade dos datos  
- controlar quen accede á información  
- manter unha organización clara e consistente  

Unity Catalog permite cubrir estas necesidades dentro de Databricks.

---

### 3.3.5 Relación co modelo de datos

Unity Catalog actúa como unha capa superior que organiza e controla o acceso ás táboas.

👉 Isto significa que:

- os datos seguen almacenándose como ficheiros (Delta Lake)  
- as táboas seguen sendo o punto de acceso principal  
- Unity Catalog engade control, organización e gobernanza sobre estes elementos  

---

## 3.4 Creación de estruturas de datos

En Databricks pódense crear catálogos, esquemas e táboas utilizando SQL.

### Crear un esquema

```sql
CREATE SCHEMA IF NOT EXISTS exemplo_esquema;
```

---

### Crear unha táboa

```sql
CREATE TABLE persoas (
    nome STRING,
    idade INT
);
```

---

### Inserir datos

```sql
INSERT INTO persoas VALUES
("Ana", 23),
("Luis", 35),
("Marta", 29);
```

---

### Consultar datos

```sql
SELECT * FROM persoas;
```

---

## 3.5 Táboas Delta

Por defecto, as táboas creadas en Databricks utilizan o formato **Delta Lake**, xa visto na unidade 1.

Isto permite:

- realizar actualizacións e borrados  
- consultar versións anteriores dos datos  
- mellorar o rendemento das consultas  

Exemplo de consulta de versións:

```sql
DESCRIBE HISTORY persoas;
```

---

## 3.6 Importación de datos

Para traballar con datos en Databricks é necesario importalos desde diferentes fontes.

Os datos poden proceder de:

- ficheiros (CSV, JSON, Parquet)  
- bases de datos  
- APIs  
- sistemas de streaming  

Nesta sección centraranse os exemplos na carga de ficheiros, que é o caso máis habitual en contornos de aprendizaxe.

---

### 3.6.1 Onde se almacenan os ficheiros

En Databricks, os ficheiros poden almacenarse en diferentes localizacións.

Na actualidade, a forma recomendada de xestionar ficheiros é mediante **volumes**, que forman parte de Unity Catalog.

Un volume é un espazo de almacenamento que permite gardar ficheiros dentro da estrutura de catálogos e esquemas:

```
catálogo → esquema → volume → ficheiros
```

Este enfoque permite integrar os ficheiros dentro do modelo de gobernanza de datos.

---

### 3.6.2 Subida de ficheiros

A forma máis habitual de cargar datos é mediante a interface gráfica de Databricks.

Proceso xeral:

1. Acceder á sección **Data ingestion**  
2. Seleccionar a opción **Upload files to a volume**  
3. Escoller o catálogo e esquema  
4. Crear ou seleccionar un volume  
5. Subir o ficheiro desde o equipo local  

Durante o proceso de carga, é necesario seleccionar:

- un catálogo  
- un esquema  
- un tipo de volume  

No caso do tipo de volume, existen dúas opcións:

- **managed volume**: Databricks xestiona automaticamente o almacenamento (opción recomendada)  
- **external volume**: os datos almacénanse nun sistema externo (uso máis avanzado)  

Para este módulo empregarase **managed volume**, xa que simplifica a xestión dos datos.

Este proceso permite dispoñer dos datos de forma inmediata dentro da plataforma.

---

### 3.6.3 Lectura de ficheiros

Unha vez cargados, os ficheiros poden lerse mediante Apache Spark.

Exemplo de lectura dun ficheiro CSV:

```python
df = spark.read.csv(
    "/Volumes/catalogo/esquema/volume/datos.csv",
    header=True,
    inferSchema=True
)

df.show()
```

Tamén se poden ler outros formatos:

```python
df = spark.read.parquet("/Volumes/catalogo/esquema/volume/datos.parquet")
```

---

### 3.6.4 Uso de DataFrames

Os datos cargados almacénanse nun **DataFrame**, que permite:

- realizar transformacións  
- aplicar filtros  
- preparar os datos para o seu almacenamento  

Este é o paso intermedio habitual antes de gardar os datos como táboa.

---

### 3.6.5 Gardar datos como táboa

Unha vez procesados, os datos poden almacenarse como táboa en Databricks.

```python
df.write.saveAsTable("persoas")
```

Isto permite:

- consultar os datos mediante SQL  
- aplicar control de acceso  
- mellorar o rendemento das consultas  

---

### 3.6.6 Nota sobre DBFS

Databricks tamén permite utilizar rutas baseadas en **DBFS**, como:

```
/FileStore/datos.csv
```

Con todo, este enfoque considérase máis tradicional. O uso de volumes é actualmente a opción recomendada, xa que se integra mellor co sistema de gobernanza de datos.

---

### 3.6.7 Resumo

O proceso típico de importación de datos en Databricks é:

```
ficheiro → volume → DataFrame → transformación → táboa
```

Este fluxo forma parte da fase de inxestión dun pipeline de datos.


## 3.7 Relación coa arquitectura modular

Nunha arquitectura modular, o almacenamento e xestión de datos realízase mediante ferramentas separadas, como:

- HDFS  
- S3  
- Delta Lake ou Iceberg  

En Databricks, estas funcionalidades están integradas nun único sistema, simplificando a xestión dos datos e a súa explotación.

---