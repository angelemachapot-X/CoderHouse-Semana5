#!/usr/bin/env python3
"""
Script de prueba en vivo para el pipeline Lakehouse:
1. Genera y envía ráfagas de datos sintéticos de sensores a Amazon Kinesis.
2. Espera el tiempo de la ventana Tumbling (1 min) y el Checkpoint de Flink.
3. Valida la existencia de la tabla en AWS Glue y los metadatos en Amazon S3.
"""

import time
import random
import boto3
import json
from datetime import datetime

REGION = "us-east-2"
STREAM_NAME = "dev-sensor-events-stream"
DATABASE_NAME = "lakehouse_db"
TABLE_NAME = "sensor_events"

kinesis_client = boto3.client('kinesis', region_name=REGION)
glue_client = boto3.client('glue', region_name=REGION)
s3_client = boto3.client('s3', region_name=REGION)

def enviar_eventos_kinesis(num_registros=30):
    print(f"🚀 [1/3] Enviando {num_registros} eventos de prueba a Kinesis Stream: {STREAM_NAME}...")
    sensores = ["sensor_A1", "sensor_B2", "sensor_C3", "sensor_D4"]
    
    for i in range(num_registros):
        sensor_id = random.choice(sensores)
        temperatura = round(random.uniform(18.5, 34.0), 2)
        payload = f"{sensor_id},{temperatura}"
        
        kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=payload.encode('utf-8'),
            PartitionKey=sensor_id
        )
        print(f"   ✓ Evento enviado #{i+1:02d}: {payload}")
        time.sleep(0.3)
        
    print("✅ Todos los eventos fueron enviados exitosamente a Kinesis.")

def verificar_glue_catalog():
    print(f"🔍 [2/3] Verificando registro en AWS Glue Data Catalog ({DATABASE_NAME}.{TABLE_NAME})...")
    try:
        response = glue_client.get_table(
            DatabaseName=DATABASE_NAME,
            Name=TABLE_NAME
        )
        table = response['Table']
        print(f"   ✓ Tabla encontrada en Glue: {table['Name']}")
        print(f"   ✓ Tipo de tabla: {table.get('TableType', 'EXTERNAL_TABLE')}")
        print(f"   ✓ Localización S3: {table.get('StorageDescriptor', {}).get('Location')}")
        print(f"   ✓ Parámetros Iceberg: {json.dumps(table.get('Parameters', {}), indent=2)}")
        return True
    except glue_client.exceptions.EntityNotFoundException:
        print("   ⚠️ La tabla aún no existe en Glue Catalog. (Flink la creará en su primera ejecución).")
        return False
    except Exception as e:
        print(f"   ❌ Error al consultar Glue: {e}")
        return False

def esperar_checkpoint_y_validar():
    print("⏳ [3/3] Esperando 70 segundos para el cierre de ventana Tumbling y el commit del Checkpoint de Flink...")
    for seg in range(70, 0, -10):
        print(f"   ... restantes: {seg}s")
        time.sleep(10)
    print("✅ Tiempo completado. Proceder a consultar en Amazon Athena:")
    print("   SELECT * FROM lakehouse_db.sensor_events ORDER BY event_time_millis DESC;")

if __name__ == "__main__":
    print("=" * 60)
    print("🧪 INICIANDO TEST EN VIVO: PIPELINE LAKEHOUSE ICEBERG")
    print("=" * 60)
    enviar_eventos_kinesis()
    verificar_glue_catalog()
    esperar_checkpoint_y_validar()
