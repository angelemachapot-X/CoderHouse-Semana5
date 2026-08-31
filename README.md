# Pre-entrega 5: Pipeline Lakehouse con Apache Iceberg, AWS Glue y Apache Flink

Este repositorio contiene la arquitectura completa como código (**Terraform**) y la aplicación de streaming (**Apache Flink 1.18 + Java**) para la persistencia transaccional y gobernanza de eventos en formato **Apache Iceberg**, integrado con el catálogo centralizado **AWS Glue Data Catalog**.

---

## 🏗️ Arquitectura de la Solución

```text
┌────────────────────────┐      ┌─────────────────────────────────┐      ┌─────────────────────────────┐
│  Dispositivos / IoT    │ ---> │   Amazon Kinesis Data Streams   │ ---> │  Amazon Managed Apache Flink│
│  (Sensor Events)       │      │   (Ingestión de streaming)      │      │  (Tumbling Window 1 min)    │
└────────────────────────┘      └─────────────────────────────────┘      └──────────────┬──────────────┘
                                                                                        │ (IcebergSink)
                                                                                        ▼
                                                                         ┌─────────────────────────────┐
                                                                         │   AWS Glue Data Catalog     │
                                                                         │   Database: lakehouse_db    │
                                                                         │   Table: sensor_events      │
                                                                         └──────────────┬──────────────┘
                                                                                        │
                                                                                        ▼
                                                                         ┌─────────────────────────────┐
                                                                         │     Amazon S3 Datalake      │
                                                                         │  s3://bucket/lakehouse/     │
                                                                         │  ├─ metadata/ (*.json,avro) │
                                                                         │  └─ data/ (*.parquet)       │
                                                                         └──────────────┬──────────────┘
                                                                                        │
                                                                                        ▼
                                                                         ┌─────────────────────────────┐
                                                                         │   Amazon Athena (Serverless)│
                                                                         │   Consultas SQL & Pruning   │
                                                                         └─────────────────────────────┘
```

---

## 📐 Estrategia de Particionado y Partition Pruning

### 1. ¿Por qué es fundamental el particionado en Iceberg?
En arquitecturas tradicionales de Data Lake basadas en directorios Hive (`year=YYYY/month=MM/...`), los motores de consulta deben listar físicamente miles de carpetas en S3 (operación `ListObjectsV2` costosa en I/O y latencia) para identificar qué archivos leer.

En cambio, **Apache Iceberg** implementa **particionado oculto (Hidden Partitioning)** y metadatos jerárquicos:
1. **Catalog**: Puntero a la versión actual de metadatos (`vN.metadata.json`).
2. **Snapshot**: Registro de operaciones atómicas (Append, Overwrite, Delete).
3. **Manifest List (`snap-*.avro`)**: Lista de manifiestos con rangos de partición (lower/upper bounds).
4. **Manifest Files (`*.avro`)**: Lista de Data Files con estadísticas a nivel de columna (mínimo, máximo, conteo de nulos).

### 2. Estrategia Elegida: Particionado Temporal (`event_time_millis` / `hour` o `day`) + Sensor
Para este pipeline de métricas de sensores, la estrategia recomendada y soportada es:
- **Partición por Tiempo (Día u Hora)**: La mayoría de las consultas analíticas en Athena buscan agregados recientes (`WHERE event_time_millis >= :start_epoch`).
- **Beneficio de Partition Pruning**:
  - Al ejecutar una consulta con filtro de fecha/hora, Athena consulta únicamente el **Manifest List**.
  - Gracias a los límites de partición guardados en los metadatos de Iceberg, el motor **descarta manifiestos completos (Manifest Pruning)** y **archivos Parquet no coincidentes (File Pruning)** sin hacer un solo `LIST` en S3 ni abrir archivos que no contienen datos relevantes.
  - **Resultado**: Reducción drástica de costos de escaneo en Athena ($5 USD por TB escaneado) y tiempos de respuesta de segundos a milisegundos.

---

## ⏱️ Mecanismo de Checkpointing en Flink y Commits en Iceberg

Uno de los errores más comunes en streaming lakehouse es la ausencia de datos en el catálogo a pesar de ver tráfico en Kinesis.

### ¿Cómo funciona el IcebergSink en Flink?
1. **Fase de Escritura (Stream Operators)**: Flink escribe los datos en archivos `.parquet` temporales en el directorio `data/` de S3.
2. **Fase de Checkpoint (Two-Phase Commit)**:
   - El `StreamExecutionEnvironment` dispara una barrera de Checkpoint (configurado a **60.000 ms / 1 minuto**).
   - El operador `IcebergFilesCommitter` recolecta todos los data files cerrados durante la ventana.
   - Crea un nuevo archivo de manifiesto (`manifest.avro`) y una nueva versión de metadatos (`vX.metadata.json`).
   - Realiza un **commit atómico** actualizando la tabla en el AWS Glue Data Catalog.
3. **Consecuencia**: Si el Checkpointing está desactivado o falla, Flink seguirá creando archivos huérfanos en S3, pero la tabla de Iceberg y Glue aparecerán completamente vacías. Por ello se exige `env.enableCheckpointing(60000)`.

---

## 🔒 Control de Concurrencia y Lock Manager en AWS Glue

Para evitar excepciones de tipo `ConcurrentModificationException`, AWS Glue Data Catalog actúa como **Optimistic Concurrency Control (OCC)**:
- Cada commit de Iceberg valida el número de versión previo.
- Si múltiples jobs o procesos concurrentes intentan actualizar la misma tabla simultáneamente, el catálogo de Glue verifica la versión atómica y fuerza un reintento del commit del snapshot sin corromper el estado de la tabla.
- Se configura en Flink:
  ```java
  catalogProps.put("type", "iceberg");
  catalogProps.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
  catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
  catalogProps.put("warehouse", "s3://" + lakehouseBucket + "/lakehouse/");
  ```

---

## 🚀 Guía de Despliegue Paso a Paso

### Paso 1: Configurar el Backend Remoto de Terraform
```bash
chmod +x infra/bootstrap/bootstrap-backend.sh
./infra/bootstrap/bootstrap-backend.sh dev-tfstate-lakehouse-bucket
```

### Paso 2: Compilar el Fat JAR de Flink (Java 11+)
```bash
cd app
mvn clean package -DskipTests
cd ..
```
Esto generará `app/target/lakehouse-streaming-job-1.0.0.jar`.

### Paso 3: Subir el JAR al bucket de artefactos en S3
```bash
aws s3 cp app/target/lakehouse-streaming-job-1.0.0.jar \
  s3://<NOMBRE_DATALAKE_BUCKET>/flink-artifacts/lakehouse-streaming-job.jar
```

### Paso 4: Desplegar la Infraestructura con Terraform
```bash
cd infra/environments/dev
terraform init
terraform plan
terraform apply -auto-approve
```

### Paso 5: Iniciar el Flink Application en AWS
```bash
aws kinesisanalyticsv2 start-application \
  --application-name dev-lakehouse-flink-job \
  --run-configuration-update file://restore-config.json \
  --region us-east-2
```

### Paso 6: Ejecutar Prueba de Ingesta y Validación
```bash
python scripts/test/prueba_en_vivo.py
```

---

## 🔍 Consultas de Validación en Amazon Athena

Una vez transcurrido al menos 1 minuto (primer commit de checkpoint):

```sql
-- 1. Consultar los eventos agregados
SELECT * FROM lakehouse_db.sensor_events ORDER BY event_time_millis DESC LIMIT 20;

-- 2. Inspeccionar el historial de Snapshots de Iceberg
SELECT * FROM "lakehouse_db"."sensor_events$snapshots";

-- 3. Inspeccionar los archivos Parquet generados y sus métricas
SELECT file_path, file_format, record_count, file_size_in_bytes 
FROM "lakehouse_db"."sensor_events$files";

-- 4. Inspeccionar el historial transaccional de commits
SELECT * FROM "lakehouse_db"."sensor_events$history";
```
