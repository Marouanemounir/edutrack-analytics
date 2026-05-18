"""
Job 1 — Ingestion : CSV locaux → MinIO Bronze (Parquet)

Ce job charge les fichiers CSV bruts dans la couche Bronze du Lakehouse.
Aucune transformation n'est appliquée : on conserve les données telles quelles.
"""
import os, sys

# Variables de configuration (injectées par Airflow ou définies ici par défaut)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
SPARK_MASTER   = os.getenv("SPARK_MASTER",     "spark://spark-master:7077")
DATA_DIR       = "/opt/data/sources"


def create_spark_session():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("Ingest_to_Bronze") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access",  "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def ingest_table(spark, table_name: str):
    """Charge un fichier CSV et l'écrit en Parquet dans Bronze."""
    csv_path     = f"{DATA_DIR}/{table_name}.csv"
    parquet_path = f"s3a://bronze/{table_name}/"

    print(f"  → Lecture : {csv_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)

    nb_lignes = df.count()
    print(f"  → Écriture : {parquet_path} ({nb_lignes} lignes)")
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"  ✓ {table_name} ingéré ({nb_lignes} lignes)")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    tables = ["students", "programs", "modules", "semesters", "grades", "absences", "activities"]

    print("=== Démarrage : Ingestion Bronze ===\n")
    for table in tables:
        ingest_table(spark, table)

    print("\n✅ Ingestion Bronze terminée avec succès")
    spark.stop()


if __name__ == "__main__":
    main()
