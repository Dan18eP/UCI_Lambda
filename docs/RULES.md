# Reglas de Oro del Proyecto (Architectural & Engineering Rules)

Este documento establece las **reglas inmutables de arquitectura, código, gobernanza e infraestructura** para el repositorio de Arquitectura Lambda de Monitoreo UCI. Todo cambio, PR o generación de código asistida por IA debe cumplir estrictamente estas directrices.

---

## 1. Reglas Arquitectónicas (Líneas Rojas del Team Leader)

1. **Cero Medallón en la Arquitectura Lambda:**
   * Queda estrictamente prohibido estructurar carpetas o capas como `bronze`, `silver` o `gold`.
   * La **Batch Layer** almacena el **Master Dataset Crudo (Append-Only)** en MinIO bajo particionado cronológico estricto:  
     `s3a://data-lake/raw-vitals/year=YYYY/month=MM/day=DD/hour=HH/`.
   * Su propósito es ser la fuente inmutable de verdad para auditorías, reconstrucción de bases de datos y entrenamiento de modelos de Machine Learning.

2. **Desacoplamiento Estricto de Airflow:**
   * Airflow orquesta **únicamente** procesos batch discretos y planificados (intervalos fijos o disparos manuales).
   * Airflow **NUNCA** orquestará streaming ni lanzará micro-batches en bucles cortos. La **Capa Speed** corre de forma autónoma 24/7 (**Always-On**) en Spark Structured Streaming manteniendo activa la Spark UI en el puerto `4040`.

3. **Capa de Servicio (Serving Layer) de Consulta Pura:**
   * PostgreSQL no es un área de preparación (*staging*).
   * Solo contiene tablas con vistas precomputadas e indexadas (`vitals_realtime` y `vitals_historico`). No se ejecutan agregaciones pesadas ni limpiezas en tiempo de consulta.

4. **Cero Carpetas de Cuarentena o Landing:**
   * La calidad de los datos se evalúa a nivel de registro mediante banderas booleanas (`is_valid_sensor = false`).
   * No se aíslan datos en directorios residuales muertos; el linaje se mantiene íntegro de principio a fin.

5. **Garantía de Idempotencia en la Capa Batch:**
   * Toda escritura de Spark Batch hacia la Capa de Servicio debe utilizar estrictamente `.mode("overwrite")` sobre la partición o clave correspondiente para que cualquier reejecución produzca el mismo resultado sin duplicidad de datos.

---

## 2. Reglas de Código y Desarrollo

1. **Calidad y Claridad de Código:**
   * Código en Python limpio, tipado (Type Hints) y documentado según PEP 8.
   * Cero "cajas negras" o librerías mágicas complejas: cada línea debe ser explicable y defendible técnicamente ante el TL.

2. **Manejo de Errores y Tolerancia a Fallos:**
   * El consumidor de Kafka y los procesos de Spark deben manejar reconexiones automáticas si el broker se reinicia.
   * Spark Structured Streaming debe utilizar siempre su ubicación de checkpoint (`s3a://data-lake/checkpoints/speed_vitals/`).

3. **Simulador Realista de 12 Camas de UCI:**
   * El generador de eventos simulará una unidad completa de 12 camas (`UCI-BED-01` a `UCI-BED-12`).
   * El flujo debe mezclar pacientes estables, pacientes con alertas preventivas, una cama con evento crítico súbito (arritmia aguda para disparar el WebSocket) y una cama con lectura anómala de sensor para evidenciar el filtrado de calidad.

---

## 3. Reglas del Frontend (React + Cero Mocks & Encapsulamiento)

1. **Stack Tecnológico:**
   * El frontend se construye en **React** (Vite + React), optimizado para renderizado reactivo de alta frecuencia y baja latencia sin recargar la página.

2. **Política Estricta de "Cero Mocks":**
   * El frontend jamás presentará datos falsos o simulados en memoria (*hardcoded*).
   * Si la Capa de Servicio está vacía o el pipeline no ha corrido, la pantalla debe mostrar un **aviso arquitectónico explícito**:  
     ` Aviso de Auditoría: Capa de Servicio vacía. Esperando que el pipeline ingiera eventos reales.`

3. **Encapsulamiento del Backend (Consumo de Solo Lectura):**
   * El frontend **no conoce** la existencia de Kafka, Spark, MinIO, Airflow, Atlas ni OpenMetadata.
   * Se comunica **únicamente** con FastAPI mediante:
     * REST: `GET /api/v1/dashboard` (carga inicial y basales históricos)
     * WebSocket: `ws://localhost:8000/ws/alerts` (notificaciones push en milisegundos)
     * Salud del Sistema: `GET /api/v1/health`
   * **Cero Botones de Disparo Batch:** El frontend no contiene botones para disparar Airflow ni intervenir en los pipelines. El flujo es 100% autónomo y desacoplado para evitar fricción y respetar el ciclo natural de la arquitectura. Airflow se administra y audita exclusivamente en su propia consola web (`localhost:8085`).

4. **WebSockets para Alertas Reactivas:**
   * En emergencias médicas de UCI, no se permite HTTP Polling (`setInterval`). Las alertas críticas se reciben mediante conexión WebSocket abierta en menos de 50 milisegundos.

5. **Barra de Auditoría Discreta para el Evaluador:**
   * El frontend incluirá una barra superior minimalista para el TL con enlaces directos de solo lectura a las consolas de infraestructura:
     * Spark UI (`4040`), Airflow (`8085`), OpenMetadata (`8585`), Atlas (`21000`), MinIO (`9001`).

---

## 4. Reglas de Gobernanza y Linaje

1. **Registro Automatizado en Apache Atlas (Puerto 21000):**
   * Se utiliza la API REST v2 de Atlas (`/api/atlas/v2/entity/bulk`) mediante el script oficial del proyecto para registrar entidades y linaje. Se evita la configuración manual de plugins Hadoop que causen fallos de classpath.

2. **Emisión de Linaje Nativo en OpenMetadata (Puerto 8585):**
   * Spark se configura con `OpenLineageSparkListener` para emitir el grafo de transformación de manera nativa e invisible a OpenMetadata.

---

## 5. Reglas para el Uso de Inteligencia Artificial

1. **Dominio Humano sobre la IA:**
   * La IA se utiliza como acelerador y validador, pero el ingeniero debe comprender, justificar y defender cada decisión técnica.
2. **Defensa Argumental:**
   * Ante cualquier pregunta del TL ("¿Por qué hiciste esto así?"), la respuesta debe basarse en principios de diseño de sistemas distribuidos, eficiencia de recursos (ej. KRaft vs ZooKeeper) e idempotencia.
