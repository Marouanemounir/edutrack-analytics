"""
Configuration partagée pour tous les jobs Spark.
Les valeurs sont lues depuis les variables d'environnement
(injectées par Docker Compose / Airflow).
"""
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
SPARK_MASTER   = os.getenv("SPARK_MASTER",     "spark://spark-master:7077")

# Chemins des 3 couches dans MinIO
BRONZE = "s3a://bronze"
SILVER = "s3a://silver"
GOLD   = "s3a://gold"

# Packages Spark nécessaires (Delta Lake + connexion S3/MinIO)
PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)


def get_spark(app_name: str):
    """Crée et retourne une SparkSession configurée."""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages",                    PACKAGES) \
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access",   "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
