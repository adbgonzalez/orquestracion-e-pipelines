# Bloque 2. Plataformas e arquitecturas para pipelines de datos

Os pipelines de datos forman parte dunha arquitectura máis ampla que integra diferentes compoñentes tecnolóxicos. Existen distintos enfoques para construír estes sistemas, dependendo das necesidades da organización, dos recursos dispoñibles e do nivel de control que se desexe ter sobre a infraestrutura.

De maneira xeral pódense identificar tres enfoques principais:

- plataformas integradas end-to-end
- plataformas especializadas
- arquitecturas modulares baseadas en ferramentas open source

---

## 1. Plataformas end-to-end

As plataformas **end-to-end** ofrecen nun único sistema a maior parte das funcionalidades necesarias para traballar con datos:

- inxestión
- almacenamento
- procesamento
- orquestración
- análise e visualización

Estas plataformas simplifican moito a construción de pipelines de datos, xa que integran todas as capas da arquitectura nun mesmo contorno.

Entre as súas vantaxes destacan:

- integración nativa entre compoñentes
- menor complexidade de despregue
- menor necesidade de administración da infraestrutura

Con todo, tamén presentan algunhas limitacións:

- menor flexibilidade na elección de ferramentas
- dependencia do provedor
- menor control sobre a infraestrutura subxacente

Algúns exemplos de plataformas deste tipo son:

- **Databricks**
- **Microsoft Fabric**
- **Google Cloud data platforms**
- **AWS analytics stack**

*(Aquí inserir un diagrama simple de arquitectura integrada.)*

---

## 2. Plataformas especializadas

Ademais das plataformas integradas, existen ferramentas e plataformas que se especializan nunha capa concreta da arquitectura de datos.

Estas solucións céntranse en resolver un problema específico de maneira eficiente, e adoitan empregarse como parte dunha arquitectura máis ampla.

Algúns exemplos son:

**Confluent**

Plataforma baseada en **Apache Kafka** orientada á integración e transmisión de datos en tempo real. Inclúe ferramentas para xestionar fluxos de eventos, conectores de datos e gobernanza de esquemas.

**Astronomer**

Plataforma orientada á xestión e despregue de **Apache Airflow**, utilizada para orquestrar workflows e pipelines de datos a gran escala.

Estas plataformas permiten simplificar a operación de ferramentas complexas e son frecuentes en contornos profesionais.

---

## 3. Arquitecturas modulares on-premise

Outra aproximación consiste en construír unha arquitectura de datos baseada en **ferramentas especializadas que se integran entre si**.

Neste modelo cada capa da arquitectura resólvese cunha ferramenta distinta.

Por exemplo:

| capa | ferramentas posibles |
|-----|----------------------|
| inxestión | NiFi, Kafka |
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

## 4. Comparación de enfoques

Os tres enfoques presentan características diferentes:

| enfoque | características |
|-------|----------------|
plataformas end-to-end | integración completa nun único sistema |
plataformas especializadas | solucións optimizadas para unha capa concreta |
arquitecturas modulares | combinación de ferramentas open source |

Na práctica, moitas organizacións empregan **arquitecturas híbridas**, combinando ferramentas open source con plataformas especializadas.

---

## 5. Enfoque empregado neste módulo

No contexto deste módulo traballarase cunha **arquitectura modular baseada en ferramentas open source**, que permite comprender mellor o funcionamento interno dos pipelines de datos.

En particular utilizarase unha arquitectura baseada nos seguintes compoñentes:

- **NiFi** para a inxestión de datos
- **Kafka** para a transmisión de eventos
- **Spark** para o procesamento distribuído
- **Delta Lake ou Iceberg** para o almacenamento
- **Airflow** para a orquestración de pipelines
- **Metabase** para a visualización de datos

Este enfoque permitirá construír pipelines completos e comprender como se integran os diferentes compoñentes dunha arquitectura de datos.

Nos seguintes apartados analizaranse con máis detalle algunhas destas ferramentas e como se utilizan para crear e xestionar pipelines de datos.