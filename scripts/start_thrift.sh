#!/bin/bash
echo "=== Démarrage Thrift Server ==="

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

echo "  → Attente démarrage (20s)..."
sleep 20

echo "  → Enregistrement des tables Gold..."
docker exec -i spark-master /opt/spark/bin/beeline \
  -u "jdbc:hive2://localhost:10000" -n "" -p "" \
  -f /opt/spark/jobs/../init_gold_tables.sql 2>/dev/null

docker cp ~/projet-bigdata/spark/init_gold_tables.sql \
  spark-master:/opt/spark/init_gold_tables.sql

docker exec -i spark-master /opt/spark/bin/beeline \
  -u "jdbc:hive2://localhost:10000" -n "" -p "" \
  -f /opt/spark/init_gold_tables.sql

echo "✅ Thrift Server prêt — tables Gold enregistrées"
