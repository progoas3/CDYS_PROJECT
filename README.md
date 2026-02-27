📊 Pipeline Medallion: Credit Event Processing
Este proyecto implementa una arquitectura de datos tipo Medallion (Bronze, Silver, Gold) utilizando PySpark. El sistema simula la ingesta de eventos de crédito en tiempo real (Streaming) y procesa los datos a través de diferentes capas para generar métricas de riesgo y un reporte final de consistencia.

🏗️ Arquitectura del Proyecto
El flujo de datos se divide en las siguientes etapas:

Landing (Simulación): Un script genera micro-batches de datos en formato CSV a partir de una fuente maestra.

Bronze (Ingesta): Captura los datos de Landing, añade metadatos de auditoría (timestamp, archivo origen) y garantiza la idempotencia evitando procesar el mismo batch dos veces.

Silver (Calidad): Limpia los datos, aplica reglas de negocio, estandariza estados de crédito y separa los registros erróneos en una tabla de Cuarentena.

Gold (Negocio): Enriquece la información con datos geográficos y genera tablas agregadas para análisis de riesgos y cohortes.

Reporte: Genera un dashboard visual en HTML con el resumen del estado de los datos.

📂 Estructura de Carpetas
Plaintext
project/
├── data/                   # Almacenamiento de capas (Parquet/CSV)
│   ├── raw_input/          # Fuente maestra (.csv)
│   ├── landing/            # Zona de llegada de batches
│   ├── bronze/             # Capa de datos crudos persistidos
│   ├── silver/             # Capa de datos limpios y válidos
│   └── gold/               # Capa de productos de datos enriquecidos
├── scripts/                # Lógica de procesamiento PySpark
│   ├── simulate_streaming.py
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── report.py
├── main.py                 # Orquestador del pipeline
└── report_final.html       # Resultado final visual
🚀 Instrucciones de Ejecución
Para un funcionamiento óptimo en Windows y para simular un entorno real de streaming, se recomienda ejecutar el sistema en dos terminales:

Paso 1: Iniciar la Ingesta (Terminal 1)
Este script simula la llegada continua de datos a la carpeta landing.

Bash
python project/scripts/simulate_streaming.py
Paso 2: Ejecutar el Pipeline (Terminal 2)
Mientras la Terminal 1 está corriendo, ejecuta el orquestador principal que procesará todas las capas de forma lineal:

Bash
python project/main.py
🛠️ Tecnologías Utilizadas
Lenguaje: Python 3.11+

Procesamiento: Apache Spark (PySpark)

Visualización: Plotly (Reporte HTML)

Gestión de Archivos: Pathlib / OS

🔍 Reglas de Calidad (Capa Silver)
loan_id: No nulo.

principal_amount: Debe ser mayor a 0.

interest_rate: Debe estar entre 0 y 1.

loan_status: Debe pertenecer al catálogo oficial (ACTIVE, DELINQUENT, etc.).

Deduplicación: Basada en la llave natural (loan_id, event_time, event_type).