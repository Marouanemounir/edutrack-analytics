"""
Job 1 — Ingestion multi-sources : CSV + PostgreSQL → MinIO Bronze (Parquet)

Sources ingérées :
  Source 1 — Fichiers CSV (7 tables académiques)
  Source 2 — PostgreSQL   (table registrations — ERP scolarité)
"""
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
SPARK_MASTER   = os.getenv("SPARK_MASTER",     "spark://spark-master:7077")
DATA_DIR       = "/opt/data/sources"

POSTGRES_URL  = "jdbc:postgresql://postgres:5432/airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "airflow_secure_2024")


def create_spark_session():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("Ingest_to_Bronze_MultiSource") \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.hadoop.fs.s3a.endpoint",         MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def ingest_csv(spark, table_name: str):
    """Source 1 — Lecture CSV → Bronze Parquet."""
    csv_path     = f"{DATA_DIR}/{table_name}.csv"
    parquet_path = f"s3a://bronze/{table_name}/"

    print(f"  [CSV] → Lecture  : {csv_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(csv_path)

    nb = df.count()
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"  [CSV] ✓ {table_name:15s} ingéré ({nb} lignes)")


def ingest_postgres(spark, table_name: str):
    """Source 2 — Lecture PostgreSQL → Bronze Parquet."""
    parquet_path = f"s3a://bronze/{table_name}/"

    print(f"  [PG]  → Lecture  : postgresql/{table_name}")
    df = spark.read \
        .format("jdbc") \
        .option("url",      POSTGRES_URL) \
        .option("dbtable",  table_name) \
        .option("user",     POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver",   "org.postgresql.Driver") \
        .load()

    nb = df.count()
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"  [PG]  ✓ {table_name:15s} ingéré ({nb} lignes)")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=== Démarrage : Ingestion Multi-Sources Bronze ===\n")

    # ── Source 1 : Fichiers CSV ───────────────────────────────────────────────
    print("--- Source 1 : Fichiers CSV ---")
    csv_tables = [
        "students", "programs", "modules", "semesters",
        "grades", "absences", "activities",
    ]
    for table in csv_tables:
        ingest_csv(spark, table)

    # ── Source 2 : PostgreSQL (ERP scolarité) ─────────────────────────────────
    print("\n--- Source 2 : PostgreSQL (ERP) ---")
    ingest_postgres(spark, "registrations")

    print("\n✅ Ingestion Bronze terminée")
    print(f"   Source 1 (CSV)        : {len(csv_tables)} tables")
    print(f"   Source 2 (PostgreSQL) : 1 table")
    print(f"   Total Bronze          : {len(csv_tables) + 1} tables")
    spark.stop()


if __name__ == "__main__":
    main()
