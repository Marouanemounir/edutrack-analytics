#!/usr/bin/env python3
"""
Crée les 3 buckets MinIO correspondant aux couches du Lakehouse :
  - bronze  : données brutes ingérées
  - silver  : données nettoyées (Delta Lake)
  - gold    : tables analytiques (Delta Lake)
"""
from minio import Minio
import os

# Connexion à MinIO (depuis la machine hôte, utilise localhost:9000)
client = Minio(
    "localhost:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    secure=False
)

buckets = ["bronze", "silver", "gold"]

print("=== Configuration des buckets MinIO ===\n")
for bucket in buckets:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"  ✓ Bucket '{bucket}' créé")
    else:
        print(f"  ℹ Bucket '{bucket}' existe déjà")

print("\n✅ MinIO configuré — Buckets : bronze, silver, gold")
