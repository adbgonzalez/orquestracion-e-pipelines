# Bloque 2. Plataformas e arquitecturas para pipelines de datos

Tal e como se viu no bloque anterior, os pipelines constrúense a partir de diferentes capas dunha arquitectura de datos. Para construír estes sistemas existen distintos enfoques, que dependen de factores como:

- o tamaño e madurez da organización  
- os recursos técnicos dispoñibles  
- o nivel de control que se desexa ter sobre a infraestrutura  
- os requisitos de escalabilidade e operación  

De forma xeral pódense identificar tres aproximacións principais para construír arquitecturas de datos:

- **plataformas integradas ou end-to-end**
- **plataformas cloud baseadas na combinación de servizos**
- **arquitecturas modulares baseadas na integración de ferramentas especializadas**

---

# 1. Plataformas end-to-end

As plataformas **end-to-end** son contornos que integran nun mesmo sistema a maior parte das capacidades necesarias para construír e operar solucións de datos.

Estas plataformas permiten deseñar, executar e xestionar **pipelines completos de datos** sen ter que integrar manualmente múltiples ferramentas independentes. Habitualmente inclúen funcionalidades para varias capas da arquitectura de datos:

- inxestión e integración de datos  
- almacenamento e xestión de datos  
- procesamento e transformación  
- orquestración de tarefas e workflows  
- análise e visualización  

Ao proporcionar estas capacidades nun único contorno, estas plataformas simplifican moito o desenvolvemento e operación de sistemas de datos.

Entre as súas principais vantaxes destacan:

- integración nativa entre compoñentes  
- menor complexidade de configuración e despregue  
- ferramentas integradas para desenvolvemento e monitorización  
- maior facilidade para escalar a infraestrutura  

Con todo, tamén presentan algunhas limitacións:

- menor flexibilidade na elección de ferramentas  
- maior dependencia dun provedor concreto  
- menor control directo sobre a infraestrutura subxacente  

---

## 1.1 Databricks

**Databricks** é unha das plataformas máis utilizadas na actualidade para o desenvolvemento de arquitecturas de datos baseadas en *lakehouse*.

Está construída arredor de **Apache Spark** e proporciona un contorno integrado para traballar con grandes volumes de datos.

Entre as súas capacidades destacan:

- desenvolvemento de pipelines de datos  
- procesamento distribuído mediante Spark  
- almacenamento optimizado mediante **Delta Lake**  
- notebooks colaborativos para análise de datos  
- execución programada de tarefas e workflows  
- ferramentas de gobernanza e control de acceso  

Databricks permite construír pipelines completos dentro da mesma plataforma, desde a integración de datos ata a análise e explotación da información.

---

## 1.2 Microsoft Fabric

**Microsoft Fabric** é unha plataforma integrada de análise de datos desenvolvida por Microsoft.

Fabric combina nun único contorno varias tecnoloxías e ferramentas orientadas ao traballo con datos, entre elas:

- integración e movemento de datos  
- almacenamento en **OneLake**  
- procesamento mediante **Spark**  
- análise mediante motores SQL  
- visualización mediante **Power BI**

A principal característica de Fabric é a integración de diferentes ferramentas nun único ecosistema, o que permite construír solucións completas de datos sen ter que integrar múltiples sistemas independentes.

A diferenza de Databricks, que está máis centrado na arquitectura lakehouse baseada en Spark, Fabric forma parte do ecosistema analítico de Microsoft e integra capacidades de análise, integración e visualización nun único sistema.

---

## 1.3 Snowflake

**Snowflake** é unha plataforma cloud de datos orientada principalmente ao almacenamento e análise de información mediante SQL.

A diferenza de Databricks, que está baseado en Apache Spark e segue un enfoque *lakehouse*, Snowflake sitúase máis próximo ao concepto de **data warehouse moderno**, cunha forte orientación á análise estruturada.

Entre as súas características destacan:

- almacenamento e procesamento desacoplados  
- alto rendemento en consultas SQL  
- xestión automática da infraestrutura  
- escalabilidade independente para almacenamento e computación  

Snowflake permite construír solucións completas de análise de datos dentro da mesma plataforma, especialmente en contornos centrados en BI e análise SQL.

---

# 2. Plataformas cloud baseadas en servizos

Outra aproximación consiste en construír arquitecturas de datos utilizando **servizos especializados dentro dunha plataforma cloud máis ampla**.

Neste caso, a plataforma non ofrece un único sistema integrado, senón múltiples servizos que se combinan para construír a arquitectura de datos.

---

## 2.1 Amazon Web Services (AWS)

Amazon Web Services ofrece unha ampla variedade de servizos orientados ao procesamento e análise de datos.

Algúns dos máis relevantes son:

- **Amazon S3**, utilizado como almacenamento de datos  
- **AWS Glue**, para integración e transformación de datos  
- **Amazon Athena**, para consultas SQL sobre data lakes  
- **Amazon EMR**, para execución de ferramentas como Spark  
- **Amazon Kinesis**, para transmisión de datos en streaming  

Estes servizos poden combinarse para construír arquitecturas completas de datos dentro do ecosistema AWS.

---

## 2.2 Google Cloud Platform (GCP)

Google Cloud tamén ofrece unha serie de ferramentas orientadas ao procesamento e análise de datos:

- **BigQuery**, sistema de análise de datos baseado en SQL  
- **Dataflow**, plataforma para procesamento de datos en streaming e batch  
- **Dataproc**, servizo para execución de Spark e Hadoop  
- **Pub/Sub**, sistema de mensaxería e transmisión de eventos  

Estas ferramentas permiten construír pipelines de datos escalables dentro da infraestrutura de Google Cloud.

---

Ademais das ferramentas open source, existen plataformas comerciais que amplían estas capacidades. Un exemplo destacado é **Confluent**, unha plataforma baseada en Apache Kafka que facilita a xestión de fluxos de datos en tempo real en contornos empresariais.

Confluent pode considerarse, en certa medida, a Kafka o que Databricks é a Spark: unha solución que simplifica o despregue, operación e escalabilidade dunha tecnoloxía distribuída amplamente utilizada.

---

# 3. Arquitecturas modulares

Outra aproximación consiste en construír unha arquitectura de datos baseada na **integración de varias ferramentas especializadas**.

Neste modelo cada capa da arquitectura resólvese cunha ferramenta distinta, que se integra co resto do sistema.

Por exemplo:

| capa | ferramentas posibles |
|-----|----------------------|
| inxestión / integración | NiFi |
| transmisión de eventos | Kafka |
| almacenamento | HDFS, Delta Lake, Iceberg |
| procesamento | Spark |
| orquestración | Airflow |
| explotación | Metabase, Superset |

Este enfoque presenta varias vantaxes:

- maior flexibilidade tecnolóxica  
- control completo sobre a infraestrutura  
- posibilidade de adaptar cada compoñente ás necesidades do sistema  

Con todo, tamén implica:

- maior complexidade de integración  
- maior responsabilidade na administración do sistema  
- maior esforzo de despregue e mantemento  

*(Aquí inserir un diagrama de arquitectura modular.)*

---

# 4. Comparación de enfoques

Os diferentes enfoques presentan características distintas:

| enfoque | idea principal | exemplo representativo |
|-------|----------------|---------|
| plataformas end-to-end | sistema integrado que cobre múltiples capas | Databricks |
| plataformas cloud baseadas en servizos | integración de varios servizos dun provedor cloud | AWS |
| arquitecturas modulares | combinación de ferramentas especializadas | NiFi + Spark + Airflow |

Na práctica, moitas organizacións empregan **arquitecturas híbridas**, combinando ferramentas open source con servizos cloud.

---

# 5. Enfoque empregado neste módulo

No contexto deste módulo traballarase principalmente cunha **arquitectura modular baseada en ferramentas open source**, que permite comprender mellor o funcionamento interno dos pipelines de datos.

En particular utilizarase unha arquitectura baseada nos seguintes compoñentes:

- **NiFi** para a inxestión de datos  
- **Kafka** para a transmisión de eventos  
- **Spark** para o procesamento distribuído  
- **Delta Lake ou Iceberg** para o almacenamento  
- **Airflow** para a orquestración de pipelines  
- **Metabase** para a visualización de datos  

Ademais, tamén se explorará o uso de **Databricks** como exemplo dunha plataforma end-to-end amplamente utilizada na industria.

Isto permitirá comparar dúas aproximacións diferentes para construír pipelines de datos:

- o uso dunha **plataforma integrada**
- o uso dunha **arquitectura modular baseada en ferramentas especializadas**

Esta comparación permitirá comprender mellor as vantaxes e limitacións de cada enfoque.
