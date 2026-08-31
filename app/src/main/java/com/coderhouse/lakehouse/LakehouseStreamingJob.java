package com.coderhouse.lakehouse;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * PRE-ENTREGA 5: Pipeline Lakehouse de Streaming con Apache Flink + Apache Iceberg + AWS Glue.
 *
 * Arquitectura del pipeline:
 * 1. Consumo de Kinesis Data Streams (eventos crudos de sensores en formato CSV "sensor_id,temperatura").
 * 2. Ventana de agregación Tumbling de 1 minuto (Processing Time) calculando el promedio de temperatura.
 * 3. Integración con AWS Glue Data Catalog como Metastore centralizado de Apache Iceberg.
 * 4. Escritura transaccional mediante IcebergSink con commits coordinados por Checkpoints (cada 60 segundos).
 */
public class LakehouseStreamingJob {

    public static void main(String[] args) throws Exception {

        // =========================================================================
        // 1. CONFIGURACIÓN DEL ENTORNO DE EJECUCIÓN Y CHECKPOINTING
        // =========================================================================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // CRUCIAL PARA ICEBERG: Flink solo realiza el commit de archivos Parquet y actualización
        // de manifiestos en Glue cuando completa un checkpoint. 60.000 ms = 1 minuto.
        env.enableCheckpointing(60_000);

        // Lectura de variables de entorno configuradas por Terraform en Managed Flink
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        if (applicationProperties == null) {
            throw new RuntimeException("KinesisAnalyticsRuntime.getApplicationProperties() devolvió null");
        }

        Properties flinkAppProps = applicationProperties.get("FlinkAppProperties");
        if (flinkAppProps == null) {
            throw new RuntimeException(
                    "No se encontró el grupo 'FlinkAppProperties'. Grupos disponibles: "
                            + applicationProperties.keySet());
        }

        String streamArn = flinkAppProps.getProperty("KINESIS_STREAM_ARN");
        String lakehouseBucket = flinkAppProps.getProperty("LAKEHOUSE_BUCKET");

        if (streamArn == null || lakehouseBucket == null) {
            throw new RuntimeException(
                    "Faltan propiedades requeridas. Claves disponibles: "
                            + flinkAppProps.stringPropertyNames()
                            + " -- streamArn=" + streamArn
                            + " lakehouseBucket=" + lakehouseBucket);
        }

        // =========================================================================
        // 2. INGESTIÓN DESDE KINESIS DATA STREAMS
        // =========================================================================
        // Extraer la región de AWS del ARN: arn:aws:kinesis:REGION:ACCOUNT:stream/NAME
        String awsRegion = streamArn.split(":")[3];

        org.apache.flink.configuration.Configuration sourceConfig =
                new org.apache.flink.configuration.Configuration();
        sourceConfig.setString("aws.region", awsRegion);

        KinesisStreamsSource<String> source = KinesisStreamsSource.<String>builder()
                .setStreamArn(streamArn)
                .setSourceConfig(sourceConfig)
                .setDeserializationSchema(new SimpleStringSchema())
                .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, recordTimestamp) -> System.currentTimeMillis());

        DataStream<String> rawEvents = env.fromSource(
                source,
                watermarkStrategy,
                "kinesis-sensor-source",
                TypeInformation.of(String.class)
        );

        // =========================================================================
        // 3. TRANSFORMACIÓN: KEYED STREAM & VENTANA TUMBLING DE 1 MINUTO
        // =========================================================================
        KeyedStream<String, String> keyed = rawEvents.keyBy(LakehouseStreamingJob::extractSensorId);

        // Ventana por Processing Time: asegura que las ventanas se cierren de forma predecible
        // incluso ante ráfagas intermitentes de tráfico de sensores.
        DataStream<String> aggregated = keyed
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .aggregate(new AverageAggregateFunction());

        // =========================================================================
        // 4. CONFIGURACIÓN DEL CATÁLOGO DE AWS GLUE PARA APACHE ICEBERG
        // =========================================================================
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("type", "iceberg");
        catalogProps.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
        catalogProps.put("warehouse", "s3://" + lakehouseBucket + "/lakehouse/");
        catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        // CatalogLoader de Iceberg usando la configuración de Glue
        CatalogLoader catalogLoader = CatalogLoader.custom(
                "glue_catalog",
                catalogProps,
                new Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog"
        );

        TableIdentifier tableId = TableIdentifier.of("lakehouse_db", "sensor_events");

        // =========================================================================
        // 5. DEFINICIÓN DEL ESQUEMA Y CREACIÓN AUTOMÁTICA DE LA TABLA ICEBERG
        // =========================================================================
        Catalog catalog = catalogLoader.loadCatalog();
        if (!catalog.tableExists(tableId)) {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "sensor_id", Types.StringType.get()),
                    Types.NestedField.required(2, "avg_temperature", Types.DoubleType.get()),
                    Types.NestedField.required(3, "event_time_millis", Types.LongType.get())
            );

            // Estrategia de Particionado para Partition Pruning:
            // Particionar por sensor_id permite a los motores analíticos como Athena
            // podar (prune) archivos parquet no solicitados al consultar sensores específicos.
            PartitionSpec spec = PartitionSpec.builderFor(schema)
                    .identity("sensor_id")
                    .build();

            catalog.createTable(tableId, schema, spec);
        }

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        // =========================================================================
        // 6. MAPPER A ROWDATA Y SINK TRANSACCIONAL DE ICEBERG
        // =========================================================================
        DataStream<RowData> rowDataStream = aggregated.map(new ToRowDataMapper());

        FlinkSink.forRowData(rowDataStream)
                .tableLoader(tableLoader)
                .append();

        // Ejecutar el pipeline de streaming
        env.execute("clase5-lakehouse-streaming-iceberg");
    }

    /**
     * Extrae el identificador del sensor a partir del payload delimitado por comas.
     */
    private static String extractSensorId(String event) {
        return event.split(",")[0].trim();
    }

    /**
     * Función de agregación stateful: calcula el promedio de temperatura por sensor.
     */
    public static class AverageAggregateFunction
            implements AggregateFunction<String, Accumulator, String> {

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator();
        }

        @Override
        public Accumulator add(String value, Accumulator acc) {
            String[] parts = value.split(",");
            acc.sensorId = parts[0].trim();
            acc.sum += Double.parseDouble(parts[1].trim());
            acc.count += 1;
            return acc;
        }

        @Override
        public String getResult(Accumulator acc) {
            double avg = acc.count == 0 ? 0.0 : acc.sum / acc.count;
            return acc.sensorId + "," + avg;
        }

        @Override
        public Accumulator merge(Accumulator a, Accumulator b) {
            Accumulator merged = new Accumulator();
            merged.sensorId = (a.sensorId != null) ? a.sensorId : b.sensorId;
            merged.sum = a.sum + b.sum;
            merged.count = a.count + b.count;
            return merged;
        }
    }

    /**
     * Acumulador serializable para el cálculo incremental del promedio.
     */
    public static class Accumulator implements Serializable {
        String sensorId;
        double sum = 0.0;
        long count = 0;
    }

    /**
     * Convierte el string resultante "sensor_id,avg_temp" al formato RowData requerido por Iceberg.
     */
    public static class ToRowDataMapper implements MapFunction<String, RowData> {
        @Override
        public RowData map(String value) {
            String[] parts = value.split(",");
            GenericRowData row = new GenericRowData(3);
            row.setField(0, StringData.fromString(parts[0].trim()));
            row.setField(1, Double.parseDouble(parts[1].trim()));
            row.setField(2, System.currentTimeMillis());
            return row;
        }
    }
}
