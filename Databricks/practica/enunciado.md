# Práctica final: pipeline de datos con Databricks

## Obxectivo

Nesta práctica proponse construír un pipeline completo en Databricks a partir dun conxunto de datos reais de uso de bicicletas compartidas.

O traballo debe incluír:

- carga de datos en bruto
- transformacións organizadas en capas `bronze`, `silver` e `gold`
- execución automática mediante un workflow
- elaboración dun **notebook dashboard** final para a exploración dos resultados

---

## Dataset de partida

O dataset empregado nesta práctica corresponde ao uso dun sistema de bicicletas compartidas e está formado por dous ficheiros CSV:

- `Databricks/data/bike-data/201508_trip_data.csv`
- `Databricks/data/bike-data/201508_station_data.csv`

O primeiro ficheiro contén os desprazamentos realizados, con información como:

- identificador da viaxe
- duración
- data e hora de inicio
- estación de saída
- estación de chegada
- tipo de usuario
- código postal

O segundo ficheiro recolle información sobre as estacións:

- identificador da estación
- nome
- coordenadas
- número de ancoraxes
- cidade
- data de instalación

Nesta práctica utilizarase a táboa de estacións para enriquecer os datos das viaxes, facendo a unión só coa estación de saída.

---

## O que se pide en cada capa

## Capa bronze

Na capa `bronze` deben cargarse os ficheiros de entrada practicamente sen transformacións, conservando a estrutura orixinal.

Espérase como mínimo:

- unha táboa `bronze` para as viaxes
- unha táboa `bronze` para as estacións
- definición explícita do esquema ou revisión do esquema inferido
- persistencia en formato Delta

O obxectivo desta capa é almacenar os datos de partida de forma trazable.

---

## Capa silver

Na capa `silver` deben realizarse as transformacións de limpeza, tipado e enriquecemento.

Espérase como mínimo:

- conversión das datas a tipos temporais adecuados
- renomeado de columnas para facilitar o traballo posterior
- revisión de valores nulos ou inconsistentes
- normalización de campos relevantes, se é necesario
- unión entre as viaxes e a táboa de estacións usando a estación de saída

Como resultado, a táboa `silver` das viaxes debe quedar enriquecida con información adicional da estación de saída, por exemplo:

- cidade de saída
- coordenadas da estación de saída
- número de ancoraxes da estación de saída

Esta capa debe representar xa un conxunto de datos limpo e consistente, apto para análise.

---

## Capa gold

Na capa `gold` deben construírse táboas agregadas orientadas á explotación analítica.

Deben existir, como mínimo, tres saídas `gold`, por exemplo arredor de:

- número de viaxes por día
- número de viaxes por cidade de saída
- distribución de viaxes por tipo de usuario ou duración media

Tamén se pode empregar unha das táboas `gold` para recoller un ranking de estacións de saída con máis uso, sempre que o conxunto final manteña tres saídas diferenciadas.

O obxectivo desta capa é dispoñer de táboas xa preparadas para alimentar visualizacións e consultas de negocio.

---

## Workflow

Debe crearse un workflow en Databricks que execute o proceso completo de forma ordenada.

Unha proposta mínima de tarefas sería:

1. carga de estacións a `bronze`
2. carga de viaxes a `bronze`
3. construción da capa `silver`
4. construción das táboas `gold`

O workflow debe reflectir as dependencias entre tarefas e permitir executar o pipeline de punta a punta.

Ademais, o notebook dashboard debe executarse despois dos notebooks `gold`, xa que debe construírse a partir das táboas finais xeradas no pipeline.

---

## Notebook dashboard final

Debe elaborarse un **notebook dashboard** a partir das táboas `gold`, seguindo unha organización clara da información.

Recoméndase esta estrutura:

### Parte superior

- título do dashboard
- filtros principais

Os filtros poden incluír, por exemplo:

- cidade de saída
- tipo de usuario
- rango temporal, se se decide incorporalo

### Zona intermedia

- KPIs ou indicadores principais

Como mínimo, poderían mostrarse:

- número total de viaxes
- duración media das viaxes
- número de cidades con actividade

### Parte inferior

- gráficas, ordenadas do máis xeral ao máis específico
- unha táboa final de apoio

Unha proposta razoable sería:

1. evolución do número de viaxes por día
2. comparación do número de viaxes por cidade de saída
3. reparto por tipo de usuario
4. ranking das estacións de saída con máis uso
5. táboa final con rexistros de detalle ou apoio

---

## Evidencias de entrega

A entrega debe realizarse nun **ficheiro comprimido** que conserve unha estrutura clara de directorios.

O ficheiro de entrega debe nomearse deste modo:

`tarefa-databricks-apelido1-apelido2-nome.zip`

Tamén se admite outro formato comprimido equivalente, por exemplo:

- `tarefa-databricks-apelido1-apelido2-nome.rar`
- `tarefa-databricks-apelido1-apelido2-nome.7z`

Recoméndase a seguinte organización:

```text
entrega/
├── notebooks/
└── capturas/
    ├── pipeline/
    ├── resultados/
    └── dashboard/
```

A continuación detállase que debe incluír cada bloque.

### 1. Notebooks

No directorio `notebooks/` deben entregarse os ficheiros dos notebooks:

- os notebooks da capa `bronze`
- o notebook da capa `silver`
- os tres notebooks da capa `gold`
- o notebook final do dashboard

### 2. Evidencias do pipeline

No directorio `capturas/pipeline/` deben entregarse capturas do workflow que mostren:

- o grafo de tarefas
- unha ou varias execucións correctas
- a configuración completa do workflow

### 3. Evidencias dos resultados

No directorio `capturas/resultados/` deben entregarse capturas coas primeiras filas das táboas xeradas:

- primeiras filas da táboa `silver`
- primeiras filas de cada unha das táboas `gold`

### 4. Evidencias do dashboard

No directorio `capturas/dashboard/` debe entregarse, como mínimo:

- unha captura do notebook dashboard final

A captura debe permitir ver a estrutura xeral do panel con filtros, KPIs, gráficas e táboa final.

---

## Rúbrica de corrección

A corrección organizarase en catro bloques e expresarase sobre `10 puntos`.

- `Notebooks`: `3 puntos`
- `Pipeline`: `3 puntos`
- `Resultados`: `2 puntos`
- `Dashboard`: `2 puntos`

En todos os apartados empregarase a mesma escala de niveis:

- **Insuficiente**: `0%`
- **Correcto**: `50%`
- **Moi bo**: `75%`
- **Excelente**: `100%`

### 1. Notebooks (`3 puntos`)

| Apartado | Puntos | Insuficiente (`0%`) | Correcto (`50%`) | Moi bo (`75%`) | Excelente (`100%`) |
|---|---:|---|---|---|---|
| Entrega e completitude | `1,5` | Faltan varios notebooks ou a entrega non permite reconstruír o traballo. | Están os notebooks principais, pero falta algún ficheiro ou parte do traballo non está claramente localizada. | Están todos os notebooks principais e a separación por fases resulta clara, aínda que hai pequenos detalles mellorables. | Están todos os notebooks pedidos e a estrutura é totalmente clara e coherente. |
| Organización do traballo nos notebooks | `1,5` | Os notebooks están desordenados ou non se corresponde ben cada un coa súa función. | A organización é entendible, pero hai mesturas entre capas ou unha estrutura pouco limpa. | A organización é boa, a separación por capas é clara e o fluxo de traballo enténdese ben. | A estrutura é moi limpa, natural e totalmente aliñada co pipeline pedido. |

### 2. Pipeline (`3 puntos`)

| Apartado | Puntos | Insuficiente (`0%`) | Correcto (`50%`) | Moi bo (`75%`) | Excelente (`100%`) |
|---|---:|---|---|---|---|
| Grafo e dependencias | `1,5` | Non hai workflow útil ou o grafo non representa ben o proceso. | Hai workflow, pero as dependencias ou a orde das tarefas non quedan suficientemente claras. | O grafo mostra unha cadea de execución clara e ben construída, con dependencias razoables. | O workflow está moi ben definido e representa con claridade todo o proceso de punta a punta. |
| Execucións e configuración | `1,5` | Faltan capturas clave ou non hai evidencia clara de execución correcta. | Hai evidencias básicas, pero a configuración ou as execucións non se poden comprobar con suficiente detalle. | As capturas permiten comprobar ben a execución e a configuración do workflow. | As evidencias son completas, claras e permiten verificar sen dúbidas o workflow. |

### 3. Resultados (`2 puntos`)

| Apartado | Puntos | Insuficiente (`0%`) | Correcto (`50%`) | Moi bo (`75%`) | Excelente (`100%`) |
|---|---:|---|---|---|---|
| Táboa `silver` | `1` | Non hai evidencia válida da táboa `silver` ou a saída non resulta coherente. | Existe a táboa `silver`, pero a evidencia é limitada ou o resultado só se aprecia parcialmente. | A saída `silver` é clara e encaixa ben co traballo pedido. | A táboa `silver` amosa de forma moi clara un resultado limpo, consistente e ben transformado. |
| Táboas `gold` | `1` | Faltan táboas `gold` ou as saídas non son adecuadas. | Existen as saídas principais, pero con pouca variedade ou utilidade limitada para análise. | As táboas `gold` están ben escollidas e son útiles para a explotación analítica. | As tres saídas `gold` son moi coherentes, están ben resoltas e alimentan claramente o dashboard final. |

### 4. Dashboard (`2 puntos`)

| Apartado | Puntos | Insuficiente (`0%`) | Correcto (`50%`) | Moi bo (`75%`) | Excelente (`100%`) |
|---|---:|---|---|---|---|
| Estrutura visual | `1` | Non hai notebook dashboard útil ou a organización é confusa. | Hai notebook dashboard, pero faltan elementos da estrutura mínima ou a disposición é pouco clara. | O notebook dashboard está ben organizado e incorpora practicamente todos os elementos pedidos. | O notebook dashboard presenta unha estrutura moi clara, equilibrada e completa, con filtros, KPIs, gráficas e táboa final. |
| Coherencia analítica | `1` | O dashboard non mostra unha lectura coherente dos datos nin unha relación clara co pipeline. | A relación co pipeline é visible, pero non queda claro que o dashboard se apoie correctamente nas saídas `gold`. | O dashboard conta ben os resultados, está claramente conectado coas táboas finais e a súa execución aparece como paso posterior ao bloque `gold`. | O dashboard presenta unha lectura analítica moi sólida, coherente, ben conectada co pipeline construído e situada con claridade como paso final despois das táboas `gold`. |

---

## Resumo final

A práctica debe mostrar un fluxo completo de traballo en Databricks:

- carga do dato bruto
- transformación en capas
- orquestración mediante workflow
- xeración de táboas analíticas
- explotación final mediante notebook dashboard

O máis importante non é engadir moitos elementos, senón que o conxunto final sexa coherente, estea ben organizado e permita ver claramente o paso de `bronze` a `silver`, de `silver` a `gold` e de `gold` á visualización final.
