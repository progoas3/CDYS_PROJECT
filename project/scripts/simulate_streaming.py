from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pathlib import Path
import time
import os

# 1. Configuración de Spark
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("TransformData")
    .getOrCreate()
)

# 2. Rutas ajustadas
base_project_path = Path(__file__).resolve().parent.parent
input_path = os.path.join(base_project_path, "data", "raw_input", "credit_events.csv")
landing_path = os.path.join(base_project_path, "data", "landing")

# 3. Lectura
df = spark.read.csv(input_path, header=True)

# Parámetros de simulación
rows_per_batch = 10
sleep_seconds = 1

# Agregar id secuencial
df = df.withColumn("row_id", monotonically_increasing_id())

total_rows = df.count()
num_batches = total_rows // rows_per_batch + 1

for batch in range(num_batches):
    batch_df = (
        df.filter(
            (df.row_id >= batch * rows_per_batch) &
            (df.row_id < (batch + 1) * rows_per_batch)
        )
        .drop("row_id")
    )

    if batch_df.count() == 0:
        continue

    output_batch_path = os.path.join(landing_path, f"batch_{batch}")

    batch_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_batch_path)

    print(f"Batch {batch} written")
    time.sleep(sleep_seconds)

spark.stop()