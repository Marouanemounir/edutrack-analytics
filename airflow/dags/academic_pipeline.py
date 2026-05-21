"""
DAG principal — Pipeline Big Data Académique
=============================================

Ordre d'exécution :
    start
      └── ingest_to_bronze    (CSV → Parquet dans MinIO Bronze)
            └── bronze_to_silver  (nettoyage → Delta Lake Silver)
                  └── silver_to_gold  (agrégations → Delta Lake Gold)
                        └── end

Planification : tous les jours à 02h00 (batch nocturne)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys

# ── Paramètres par défaut du DAG ──────────────────────────────────────────
default_args = {
    "owner":            "bigdata-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "on_failure_callback": lambda context: print(
        f"❌ ÉCHEC : {context['task_instance'].task_id} "
        f"à {context['execution_date']}"
    ),
}


def run_spark_job(module_name: str, **kwargs):
    """
    Importe et exécute le job Spark spécifié.
    Les jobs sont dans /opt/spark/jobs/ (monté via Docker volume).
    """
    import importlib
    sys.path.insert(0, "/opt/spark/jobs")
    module = importlib.import_module(module_name)
    module.main()


# ── Définition du DAG ─────────────────────────────────────────────────────
with DAG(
    dag_id="academic_pipeline",
    default_args=default_args,
    description="Pipeline Big Data — Pilotage académique (Bronze → Silver → Gold)",
    schedule_interval="0 2 * * *",  # Chaque nuit à 02h00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["academic", "bigdata", "lakehouse", "delta-lake"],
    doc_md="""
    ## Pipeline Académique Big Data

    Ce pipeline orchestre l'ingestion et la transformation des données académiques :

    1. **Bronze** : Chargement des CSV bruts vers MinIO
    2. **Silver** : Nettoyage et normalisation (Delta Lake)
    3. **Gold** : Calcul des KPIs académiques (Delta Lake)
    """,
) as dag:

    # ── Tâches ──────────────────────────────────────────────────────────────
    start = EmptyOperator(
        task_id="start",
        doc_md="Point de départ du pipeline"
    )

    end = EmptyOperator(
        task_id="end",
        doc_md="Fin du pipeline — données Gold disponibles"
    )

    ingest_bronze = PythonOperator(
        task_id="ingest_to_bronze",
        python_callable=run_spark_job,
        op_kwargs={"module_name": "ingest_to_bronze"},
        doc_md="Ingestion des CSV depuis data/sources/ vers MinIO Bronze (Parquet)",
    )

    bronze_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_spark_job,
        op_kwargs={"module_name": "bronze_to_silver"},
        doc_md="Nettoyage et normalisation Bronze → Silver (Delta Lake)",
    )

    silver_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_spark_job,
        op_kwargs={"module_name": "silver_to_gold"},
        doc_md="Calcul des KPIs et agrégations Silver → Gold (Delta Lake)",
    )

    quality_check = PythonOperator(
    task_id="quality_check",
    python_callable=run_spark_job,
    op_kwargs={"module_name": "quality_check"},
    doc_md="Contrôle qualité des données Bronze avant transformation",
    )
    # Nouvelle chaîne
    start >> ingest_bronze >> quality_check >> bronze_silver >> silver_gold >> end
