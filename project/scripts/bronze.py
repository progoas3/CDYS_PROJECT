import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract, col
from pyspark.sql.utils import AnalysisException
from pathlib import Path


# 1. Configuración de Spark
spark = (
    SparkSession.builder
    .appName("bronze-layer")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Rutas Dinámicas (Ajustadas a tu estructura)
# Navegamos de project/scripts/ -> project/data/
base_project_path = Path(__file__).resolve().parent.parent
data_path = os.path.join(base_project_path, "data", "landing")
bronze_path = os.path.join(base_project_path, "data", "bronze")

print(f"Reading from: {data_path}")
print(f"Writing to: {bronze_path}")

# 3. Lógica para evitar duplicados (Idempotencia)
batches_procesados = []
if os.path.exists(bronze_path):
    try:
        existing_data = spark.read.parquet(bronze_path)
        batches_procesados = [row.batch_id for row in existing_data.select("batch_id").distinct().collect()]
        print(f"Batches ya existentes en Bronze: {batches_procesados}")
    except AnalysisException:
        batches_procesados = []

# 4. Lectura de datos (Capa Landing)
try:
    df_landing = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(data_path)
    )

    # 5. Inclusión de metadatos y filtrado de nuevos datos
    df_bronze = (
        df_landing
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("batch_id", regexp_extract("source_file", r"(batch_(\d+))", 1))
    )

    # Filtrar solo lo que NO se ha procesado
    df_nuevos_datos = df_bronze.filter(~col("batch_id").isin(batches_procesados))

    # 6. Almacenamiento
    cantidad_nuevos = df_nuevos_datos.count()

    if cantidad_nuevos > 0:
        print(f"Procesando {cantidad_nuevos} registros nuevos...")
        (
            df_nuevos_datos
            .repartition(1)
            .write
            .mode("append")
            .format("parquet")
            .partitionBy("batch_id")
            .save(bronze_path)
        )
        print("Datos guardados exitosamente en Bronze.")
    else:
        print("Idempotencia aplicada: No se encontraron batches nuevos.")

except Exception as e:
    print(f"Error en el proceso de Bronze: {e}")

finally:
    spark.stop()
