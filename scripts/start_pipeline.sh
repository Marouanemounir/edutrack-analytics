#!/bin/bash
echo "=== Démarrage du pipeline EduTrack ==="

cd ~/projet-bigdata

# 1. Démarrer la stack
docker compose up -d
echo "✓ Stack Docker démarrée"
sleep 10

# 2. Créer les buckets MinIO
python3 scripts/setup_minio.py

# 3. Lancer le pipeline
docker exec airflow-webserver python3 /opt/spark/jobs/ingest_to_bronze.py
docker exec airflow-webserver python3 /opt/spark/jobs/bronze_to_silver.py
docker exec airflow-webserver python3 /opt/spark/jobs/silver_to_gold.py

# 4. Démarrer le Thrift Server
docker exec -d spark-master bash -c "
  /opt/spark/sbin/start-thriftserver.sh \
    --master local[2] \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --hiveconf hive.server2.thrift.port=10000 \
    --hiveconf hive.server2.thrift.bind.host=0.0.0.0
"
sleep 20

# 5. Enregistrer les tables Gold
docker exec -i spark-master /opt/spark/bin/beeline \
  -u "jdbc:hive2://localhost:10000" -n "" -p "" << 'SQL'
CREATE TABLE IF NOT EXISTS gold_success_rate USING DELTA LOCATION 's3a://gold/success_rate/';
CREATE TABLE IF NOT EXISTS gold_absenteeism USING DELTA LOCATION 's3a://gold/absenteeism/';
CREATE TABLE IF NOT EXISTS gold_student_performance USING DELTA LOCATION 's3a://gold/student_performance/';
CREATE TABLE IF NOT EXISTS gold_program_kpis USING DELTA LOCATION 's3a://gold/program_kpis/';
CREATE TABLE IF NOT EXISTS gold_activity_summary USING DELTA LOCATION 's3a://gold/activity_summary/';
CREATE TABLE IF NOT EXISTS gold_correlation_abs_grades USING DELTA LOCATION 's3a://gold/correlation_abs_grades/';
CREATE TABLE IF NOT EXISTS gold_semester_dashboard USING DELTA LOCATION 's3a://gold/semester_dashboard/';
SQL

echo ""
echo "✅ Pipeline prêt — Interfaces disponibles :"
echo "   Airflow  : http://$(curl -s ifconfig.me):8080"
echo "   MinIO    : http://$(curl -s ifconfig.me):9001"
echo "   Superset : http://$(curl -s ifconfig.me):8088"
echo "   Spark UI : http://$(curl -s ifconfig.me):8082"
