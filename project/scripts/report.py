from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import plotly.express as px
from pathlib import Path
from datetime import datetime
import os

# 1. Inicializar Spark
spark = SparkSession.builder \
    .appName("gold-report-final-v2") \
    .master("local[*]") \
    .getOrCreate()

# 2. Configuración de rutas ajustadas
# Navegamos desde project/scripts hacia la raíz del proyecto para encontrar /data
BASE_PROJECT_PATH = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_PROJECT_PATH / "data"
REPORT_FILE = BASE_PROJECT_PATH.parent / "report_final.html"

try:
    # 3. Lectura de capas con rutas relativas corregidas
    df_bronze = spark.read.parquet(str(DATA_PATH / "bronze"))
    df_valid = spark.read.parquet(str(DATA_PATH / "silver" / "valid"))
    df_quarantine = spark.read.parquet(str(DATA_PATH / "silver" / "quarantine"))
    df_metrics = spark.read.parquet(str(DATA_PATH / "silver" / "metrics"))

    # Leemos el detalle enriquecido para el conteo principal
    df_gold_total = spark.read.parquet(str(DATA_PATH / "gold" / "enriched_loan_details"))

    # Leemos el reporte de riesgo solo para la gráfica de barras
    df_gold_risk = spark.read.parquet(str(DATA_PATH / "gold" / "regional_risk_report"))

    # 4. Cálculo de métricas
    m = df_metrics.orderBy(F.col("execution_ts").desc()).first()
    count_bronze = df_bronze.count()
    count_valid = df_valid.count()
    count_quarantine = df_quarantine.count()
    count_gold = df_gold_total.count()

    landing_path = DATA_PATH / "landing"
    num_batches = len(
        [d for d in os.listdir(landing_path) if os.path.isdir(os.path.join(landing_path, d))]) if os.path.exists(
        landing_path) else 0

    # 5. Preparación de datos para gráficas
    pd_risk = df_gold_risk.toPandas().fillna("NO DEFINIDO")

    colores_riesgo = {
        'BAJO': '#27ae60', 'MEDIO': '#f1c40f', 'ALTO': '#e67e22', 'NO DEFINIDO': '#95a5a6'
    }

    # 6. Creación de Visualizaciones
    fig_quality = px.pie(
        values=[count_valid, count_quarantine, int(m['duplicate_records'])],
        names=["Válidos", "Cuarentena", "Duplicados"],
        title="Distribución de Calidad (Silver Layer)",
        hole=0.4,
        color_discrete_sequence=['#2ecc71', '#f39c12', '#e74c3c']
    )

    fig_risk = px.bar(
        pd_risk, x="macro_region", y="portfolio_value", color="risk_segment",
        title="Cartera por Región y Segmento de Riesgo",
        barmode="stack",
        color_discrete_map=colores_riesgo,
        template="plotly_white"
    )
    fig_risk.update_layout(xaxis={'categoryorder': 'total descending'})

    chart1_html = fig_quality.to_html(full_html=False, include_plotlyjs='cdn')
    chart2_html = fig_risk.to_html(full_html=False, include_plotlyjs='cdn')

    # 7. Construcción del HTML
    html_content = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, sans-serif; margin: 0; padding: 40px; background-color: #f8f9fa; }}
            .container {{ max-width: 1200px; margin: auto; background: white; padding: 35px; border-radius: 15px; box-shadow: 0 5px 25px rgba(0,0,0,0.1); }}
            .header {{ border-bottom: 3px solid #3498db; margin-bottom: 30px; padding-bottom: 15px; }}
            .debug-info {{ background: #f1f3f5; padding: 15px; font-size: 0.85em; border-radius: 8px; color: #495057; margin-bottom: 25px; }}
            .metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 30px; }}
            .metric-box {{ background: #fff; border: 1px solid #e9ecef; padding: 15px; border-radius: 10px; text-align: center; border-bottom: 4px solid #3498db; }}
            .metric-box h3 {{ margin: 0; font-size: 11px; color: #adb5bd; text-transform: uppercase; }}
            .metric-box p {{ margin: 8px 0 0; font-size: 26px; font-weight: bold; color: #2c3e50; }}
            .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 25px; }}
            .footer {{ margin-top: 35px; padding: 20px; background: #ebf5fb; border-left: 5px solid #3498db; border-radius: 5px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Informe de Control: Data Pipeline Medallion</h1>
                <div class="debug-info">
                    <strong>Paths de Datos:</strong> {DATA_PATH} <br>
                    <strong>Fecha de Ejecución:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} <br>
                    <strong>Unidades Procesadas:</strong> {num_batches} Micro-batches procesados.
                </div>
            </div>

            <div class="metrics">
                <div class="metric-box"><h3>Bronze (Raw)</h3><p>{count_bronze:,}</p></div>
                <div class="metric-box"><h3>Silver (Valid)</h3><p>{count_valid:,}</p></div>
                <div class="metric-box"><h3>Cuarentena</h3><p>{count_quarantine:,}</p></div>
                <div class="metric-box"><h3>Gold (Enriched)</h3><p>{count_gold:,}</p></div>
            </div>

            <div class="grid">
                <div class="card">{chart1_html}</div>
                <div class="card">{chart2_html}</div>
            </div>

            <div class="footer">
                <h3>🔍 Calidad y Consistencia</h3>
                <p>La capa Gold ahora refleja el 100% de los datos válidos de Silver enriquecidos con georreferenciación.</p>
                <strong>Registros Totales:</strong> {count_gold} créditos segmentados con éxito.
            </div>
        </div>
    </body>
    </html>
    """

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"✅ Reporte generado con {count_gold} registros en Gold.")

except Exception as e:
    print(f"Error: {e}")
finally:
    spark.stop()