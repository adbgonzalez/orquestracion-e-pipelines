# Índice de Airflow

Este bloque reúne os materiais introdutorios sobre Apache Airflow dentro do proxecto. Os capítulos están organizados para avanzar desde a visión xeral da ferramenta ata a escritura de DAGs, a comunicación entre tarefas e o uso de operadores máis específicos.

## Contidos

1. [Introdución a Apache Airflow](./1.intro-airflow.md)

   Presentación xeral de Airflow, o seu papel como ferramenta de orquestración e a diferenza entre DAGs e scripts secuenciais.

2. [Conceptos básicos de Airflow](./2.conceptos-basicos.md)

   Explicación da terminoloxía fundamental: `DAG`, `task`, `operator`, dependencias, `scheduler`, `executor`, `dag run` e `task instance`.

3. [Airflow no proxecto](./3.airflow-no-proxecto.md)

   Descrición da integración de Airflow no stack do laboratorio, a imaxe empregada, os providers instalados e a relación co resto de servizos do clúster.

4. [A interface web de Airflow](./4.interfaz-airflow.md)

   Percorrido pola interface web, vistas principais, execución manual, logs, conexións, variables e primeiros comandos útiles da CLI.

5. [Primeiros DAGs en Airflow](./5.primeiros-dags.md)

   Introdución á escritura de DAGs sinxelos, estrutura básica dos ficheiros e primeiros exemplos con tarefas Python e Bash.

6. [Comunicación entre tarefas en Airflow](./6.comunicacion-entre-tarefas.md)

   Explicación de `XCom`, ficheiros como artefactos intermedios, `params`, `Variables`, `Connections`, `templates`, `branching` e `trigger rules`.

7. [Operadores específicos en Airflow](./7.operadores-especificos.md)

   Revisión de operadores especialmente relevantes para o proxecto, como `PythonOperator`, `BashOperator`, `EmptyOperator`, `SparkSubmitOperator`, operadores de Kafka e `HttpOperator`.

## Suxestión de lectura

Unha orde natural para seguir estes materiais sería esta:

1. introdución xeral
2. conceptos básicos
3. integración de Airflow no proxecto
4. interface web e CLI
5. primeiros DAGs
6. comunicación entre tarefas
7. operadores específicos

Deste modo, primeiro constrúese o modelo mental da ferramenta, despois vese como se integra no contorno do laboratorio e, finalmente, avánzase cara á práctica con DAGs e operadores reais.
