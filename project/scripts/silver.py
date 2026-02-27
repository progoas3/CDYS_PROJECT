from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, when, current_timestamp, lit, row_number, upper
)
from pyspark.sql.window import Window
from pathlib import Path

# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("silver-layer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────
# Rutas Ajustadas
# ─────────────────────────────────────────────
# Navegamos desde project/scripts hacia project/data
BASE_PROJECT_PATH = Path(__file__).resolve().parent.parent

BRONZE_PATH = BASE_PROJECT_PATH / "data" / "bronze"
SILVER_VALID_PATH = BASE_PROJECT_PATH / "data" / "silver" / "valid"
SILVER_QUARANTINE_PATH = BASE_PROJECT_PATH / "data" / "silver" / "quarantine"
SILVER_METRICS_PATH = BASE_PROJECT_PATH / "data" / "silver" / "metrics"

# Asegurar que existan los directorios de salida
SILVER_VALID_PATH.mkdir(parents=True, exist_ok=True)
SILVER_QUARANTINE_PATH.mkdir(parents=True, exist_ok=True)
SILVER_METRICS_PATH.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# Leer Bronze
# ─────────────────────────────────────────────
df = spark.read.parquet(str(BRONZE_PATH))

# ─────────────────────────────────────────────
# Reglas de calidad
# ─────────────────────────────────────────────
valid_statuses = ["ACTIVE", "DELINQUENT", "CLOSED", "DEFAULTED", "PAYMENT", "DELINQUENCY"]

df = df.withColumn(
    "is_valid",
    (col("loan_id").isNotNull()) &
    (col("event_time").isNotNull()) &
    (col("principal_amount") > 0) &
    (upper(col("loan_status")).isin(valid_statuses)) &
    (col("interest_rate").between(0, 1)) &
    (col("term_months") > 0)
)

# ─────────────────────────────────────────────
# Duplicados (llave natural)
# ─────────────────────────────────────────────
window = Window.partitionBy(
    "loan_id", "event_time", "event_type"
).orderBy(col("ingestion_timestamp"))

df = df.withColumn(
    "row_number",
    row_number().over(window)
)

# ─────────────────────────────────────────────
# Cuarentena
# ─────────────────────────────────────────────
quarantine_df = (
    df.filter(
        (col("is_valid") == False) | (col("row_number") > 1)
    )
    .withColumn(
        "quarantine_reason",
        when(col("is_valid") == False, lit("INVALID_RULES"))
        .otherwise(lit("DUPLICATE"))
    )
)

# ─────────────────────────────────────────────
# Registros válidos
# ─────────────────────────────────────────────
valid_df = (
    df.filter(
        (col("is_valid") == True) & (col("row_number") == 1)
    )
    .drop("is_valid", "row_number")
)

# ─────────────────────────────────────────────
# Escritura Silver
# ─────────────────────────────────────────────
valid_df.write.mode("overwrite").parquet(str(SILVER_VALID_PATH))
quarantine_df.write.mode("overwrite").parquet(str(SILVER_QUARANTINE_PATH))

# ─────────────────────────────────────────────
# Métricas de calidad
# ─────────────────────────────────────────────
metrics_df = (
    df.agg(
        count("*").alias("total_records"),
        sum((col("row_number") > 1).cast("int")).alias("duplicate_records"),
        sum((col("is_valid") == False).cast("int")).alias("invalid_records"),
        (sum(col("loan_id").isNull().cast("int")) / count("*")).alias("null_pct_loan_id"),
        (sum(col("principal_amount").isNull().cast("int")) / count("*")).alias("null_pct_principal_amount"),
        (sum(col("loan_status").isNull().cast("int")) / count("*")).alias("null_pct_loan_status")
    )
    .withColumn("execution_ts", current_timestamp())
)

metrics_df.write.mode("overwrite").parquet(str(SILVER_METRICS_PATH))

spark.stop()