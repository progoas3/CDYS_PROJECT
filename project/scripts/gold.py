from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, month, year, concat, lit, when
from pathlib import Path

# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("gold-layer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────
# Rutas Ajustadas
# ─────────────────────────────────────────────
# Navegamos desde project/scripts hacia project/data
BASE_PROJECT_PATH = Path(__file__).resolve().parent.parent

SILVER_PATH = BASE_PROJECT_PATH / "data" / "silver" / "valid"
REFERENCE_PATH = BASE_PROJECT_PATH / "data" / "raw_input" / "region_reference.csv"
GOLD_PATH = BASE_PROJECT_PATH / "data" / "gold"

GOLD_PATH.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# Cargar datos de Silver y Referencia
# ─────────────────────────────────────────────
df_silver = spark.read.parquet(str(SILVER_PATH))
df_ref = spark.read.option("header", "true").csv(str(REFERENCE_PATH))

# Enriquecimiento (Join con Catálogo de Regiones)
df_enriched = df_silver.join(df_ref, on="region", how="left")

# ─────────────────────────────────────────────
# Producto de Datos 0 - Detalle Enriquecido (Gold Bruto)
# ─────────────────────────────────────────────
# Guarda los registros individuales enriquecidos (consistencia de datos)
df_enriched.write.mode("overwrite").parquet(str(GOLD_PATH / "enriched_loan_details"))

# ─────────────────────────────────────────────
# Producto de Datos 1: Análisis de Cosechas (Cohortes)
# ─────────────────────────────────────────────
cohort_analysis = (
    df_enriched
    .withColumn("cohort", concat(year("event_time"), lit("-"), month("event_time")))
    .groupBy("cohort", "product_type")
    .agg(
        count("loan_id").alias("total_loans"),
        sum("principal_amount").alias("total_disbursed"),
        avg("interest_rate").alias("avg_rate"),
        sum(when(col("loan_status") == "delinquent", 1).otherwise(0)).alias("delinquent_cases")
    )
    .orderBy("cohort")
)

# ─────────────────────────────────────────────
# Producto de Datos 2: Vista Consolidada de Riesgo Regional
# ─────────────────────────────────────────────
regional_risk_report = (
    df_enriched
    .groupBy("macro_region", "risk_segment", "loan_status")
    .agg(
        sum("outstanding_balance").alias("portfolio_value"),
        count("loan_id").alias("loan_count")
    )
    .orderBy("macro_region", "risk_segment")
)

# ─────────────────────────────────────────────
# Almacenamiento Final
# ─────────────────────────────────────────────
cohort_analysis.write.mode("overwrite").parquet(str(GOLD_PATH / "cohort_analysis"))
regional_risk_report.write.mode("overwrite").parquet(str(GOLD_PATH / "regional_risk_report"))

spark.stop()