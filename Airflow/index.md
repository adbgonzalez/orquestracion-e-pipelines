# Apuntamentos de Airflow

Este bloque reune os materiais introdutorios sobre Apache Airflow dentro do proxecto. Os capitulos estan organizados para avanzar desde a vision xeral da ferramenta ata a escritura de DAGs, a comunicacion entre tarefas, o uso de operadores mais especificos e a planificacion temporal dos fluxos.

## Contidos

1. [Introducion a Apache Airflow](./1.intro-airflow.md)

   Presentacion xeral de Airflow, o seu papel como ferramenta de orquestracion e a diferenza entre DAGs e scripts secuenciais.

2. [Conceptos basicos de Airflow](./2.conceptos-basicos.md)

   Explicacion da terminoloxia fundamental: `DAG`, `task`, `operator`, dependencias, `scheduler`, `executor`, `dag run` e `task instance`.

3. [Airflow no proxecto](./3.airflow-no-proxecto.md)

   Descricion da integracion de Airflow no stack do laboratorio, a imaxe empregada, os providers instalados e a relacion co resto de servizos do cluster.

4. [A interface web de Airflow](./4.interfaz-airflow.md)

   Percorrido pola interface web, vistas principais, execucion manual, logs, conexions, variables e primeiros comandos utiles da CLI.

5. [Primeiros DAGs en Airflow](./5.primeiros-dags.md)

   Introducion a escritura de DAGs sinxelos, estrutura basica dos ficheiros e primeiros exemplos con tarefas Python e Bash.

6. [Comunicacion entre tarefas en Airflow](./6.comunicacion-entre-tarefas.md)

   Explicacion de `XCom`, ficheiros como artefactos intermedios, `params`, `Variables`, `Connections`, `templates`, `branching` e `trigger rules`.

7. [Operadores especificos en Airflow](./7.operadores-especificos.md)

   Revision de operadores especialmente relevantes para o proxecto, como `PythonOperator`, `BashOperator`, `EmptyOperator`, `SparkSubmitOperator`, operadores de Kafka e `HttpOperator`.

8. [`Schedule`, tempo e planificacion en Airflow](./8.schedule-e-planificacion.md)

   Introducion a planificacion temporal dos DAGs: `start_date`, `schedule`, expresions cron, `logical_date`, intervalos de datos e `catchup`.

## Bibliografia

- de Ruiter, Julian, Ismael Cabral, Kris Geusebroek, Daniel van der Ende e Bas Harenslak. *Data Pipelines with Apache Airflow. Second Edition*. Manning, 2026.
- Apache Airflow. *Official Documentation*. https://airflow.apache.org/docs/
- Apache Airflow. *Apache Airflow Core Documentation*. https://airflow.apache.org/docs/apache-airflow/stable/
- Apache Airflow. *Provider Packages Documentation*. https://airflow.apache.org/docs/#providers-packages-docs
