# EduTrack Analytics — Plateforme Big Data de Pilotage Académique

> Projet de fin de module **Big Data** — II-BDCC  
> Pipeline Lakehouse batch orchestré pour l'analyse des données académiques

---

## Sommaire

- [Présentation](#présentation)
- [Architecture](#architecture)
- [Stack technique](#stack-technique)
- [Structure du projet](#structure-du-projet)
- [Données](#données)
- [Pipeline de données](#pipeline-de-données)
- [Lancement rapide](#lancement-rapide)
- [Interfaces](#interfaces)
- [Indicateurs analytiques](#indicateurs-analytiques)
- [Commandes utiles](#commandes-utiles)

---

## Présentation

**EduTrack Analytics** est une plateforme Big Data de type **Lakehouse** conçue pour centraliser, transformer et analyser les données académiques de trois filières : **BDCC**, **CCN** et **GLSID**.

Elle permet de calculer des indicateurs de pilotage pédagogique tels que :
- le taux de réussite par module et par filière
- le suivi de l'absentéisme étudiant
- la performance individuelle et collective
- l'engagement sur les activités pédagogiques

Le pipeline est **entièrement orchestré par Apache Airflow** et s'exécute automatiquement chaque nuit en mode batch.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCES DE DONNÉES                           │
│   students.csv  │  grades.csv  │  absences.csv  │  activities.csv  │
│   programs.csv  │  modules.csv │  semesters.csv                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION — Airflow                        │
│                                                                     │
│   start ──► ingest_to_bronze ──► bronze_to_silver ──► silver_to_gold ──► end
│                                                                     │
│   Planification : quotidienne à 02h00 (cron: 0 2 * * *)            │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
         ┌─────────┐    ┌──────────┐    ┌──────────┐
         │  BRONZE │    │  SILVER  │    │   GOLD   │
         │ Parquet │───►│  Delta   │───►│  Delta   │
         │  brut   │    │ nettoyé  │    │  KPIs    │
         └─────────┘    └──────────┘    └──────────┘
               │               │               │
               └───────────────┴───────────────┘
                                       │
                               ┌───────▼───────┐
                               │     MinIO     │
                               │  (S3-compat.) │
                               └───────┬───────┘
                                       │
                         ┌─────────────▼────────────┐
                         │   Spark Thrift Server    │
                         │      (JDBC / SQL)        │
                         └─────────────┬────────────┘
                                       │
                               ┌───────▼───────┐
                               │   Superset    │
                               │  Dashboards   │
                               └───────────────┘
```

---

## Stack technique

| Technologie | Version | Rôle |
|-------------|---------|------|
| **Apache Airflow** | 2.9.0 | Orchestration et planification des traitements batch |
| **Apache Spark** | 3.5.0 | Traitement Big Data batch (ETL Bronze → Silver → Gold) |
| **Delta Lake** | 3.2.0 | Format transactionnel pour les tables analytiques |
| **MinIO** | latest | Stockage objet S3-compatible (couche physique du Lakehouse) |
| **Spark Thrift Server** | 3.5.0 | Exposition SQL des tables Delta via JDBC/ODBC |
| **Apache Superset** | 3.1.0 | Visualisation et tableaux de bord analytiques |
| **PostgreSQL** | 15 | Base de métadonnées pour Airflow |
| **Docker Compose** | v2 | Déploiement unifié de toute la plateforme |

---

## Structure du projet

```
projet-bigdata/
├── .env                        ← Variables secrètes (non versionné)
├── .gitignore
├── docker-compose.yml          ← Définition de tous les services
├── README.md
│
├── airflow/
│   ├── Dockerfile              ← Image Airflow + Java + PySpark
│   └── dags/
│       └── academic_pipeline.py  ← DAG principal d'orchestration
│
├── spark/
│   ├── Dockerfile              ← Image Spark + Delta Lake JARs
│   └── jobs/
│       ├── config.py           ← Configuration Spark partagée
│       ├── ingest_to_bronze.py ← Job 1 : CSV → Parquet Bronze
│       ├── bronze_to_silver.py ← Job 2 : nettoyage → Delta Silver
│       └── silver_to_gold.py   ← Job 3 : KPIs → Delta Gold
│
├── data/
│   └── sources/                ← 7 fichiers CSV sources
│       ├── students.csv
│       ├── programs.csv
│       ├── modules.csv
│       ├── semesters.csv
│       ├── grades.csv
│       ├── absences.csv
│       └── activities.csv
│
└── scripts/
    └── setup_minio.py          ← Création des buckets MinIO
```

---

## Données

| Fichier | Lignes | Description |
|---------|--------|-------------|
| `students.csv` | 358 | Étudiants des 3 filières (BDCC, CCN, GLSID), 3 cohortes |
| `programs.csv` | 3 | Filières avec responsable et département |
| `modules.csv` | 78 | Modules par filière avec coefficient et difficulté |
| `semesters.csv` | 12 | Semestres S1–S6, années 2023-2026 |
| `grades.csv` | 6 686 | Notes (session Normal / Rattrapage), entre 0 et 20 |
| `absences.csv` | 9 643 | Absences avec heures et statut (justifiée / non justifiée) |
| `activities.csv` | 16 577 | Activités pédagogiques (Quiz, Project, Assignment, PFE…) |

**Total : ~33 000 enregistrements sur 3 cohortes académiques (2023–2026)**

---

## Pipeline de données

### Couche Bronze — Données brutes

Ingestion directe des CSV sans transformation. Format Parquet.

```
s3a://bronze/students/
s3a://bronze/programs/
s3a://bronze/modules/
s3a://bronze/semesters/
s3a://bronze/grades/
s3a://bronze/absences/
s3a://bronze/activities/
```

### Couche Silver — Données nettoyées (Delta Lake)

Transformations appliquées :
- Suppression des doublons
- Normalisation des formats de dates (`yyyy-MM-dd`)
- Normalisation des chaînes (trim, upper/lower)
- Filtrage des valeurs aberrantes (notes hors [0, 20])
- Conversion du champ `justified` : `"Yes"/"No"` → booléen `is_justified`
- Ajout de colonnes calculées : `mention`, `is_passed`

```
s3a://silver/students/
s3a://silver/programs/
s3a://silver/modules/
s3a://silver/semesters/
s3a://silver/grades/
s3a://silver/absences/
s3a://silver/activities/
```

### Couche Gold — Tables analytiques (Delta Lake)

| Table | Description |
|-------|-------------|
| `gold_success_rate` | Taux de réussite, moyenne, écart-type par module |
| `gold_absenteeism` | Total heures d'absence par étudiant |
| `gold_student_performance` | Performance individuelle + catégorie (Excellent / Bon / Passable / En difficulté) |
| `gold_program_kpis` | KPIs agrégés par filière et niveau |
| `gold_activity_summary` | Engagement par type d'activité et plateforme |

```
s3a://gold/success_rate/
s3a://gold/absenteeism/
s3a://gold/student_performance/
s3a://gold/program_kpis/
s3a://gold/activity_summary/
```

---

## Lancement rapide

### Prérequis

- Machine Ubuntu (Oracle Cloud ou locale) avec **8 GB RAM minimum**
- Docker et Docker Compose installés
- Les 7 fichiers CSV dans `data/sources/`

### 1 — Cloner le projet

```bash
git clone https://github.com/Marouanemounir/edutrack-analytics.git
cd edutrack-analytics
```

### 2 — Configurer les variables d'environnement

```bash
cp .env.example .env
# Éditer .env et remplir les clés Fernet et les mots de passe
nano .env
```

> Générer la clé Fernet :
> ```bash
> python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
> ```

### 3 — Copier les fichiers CSV

```bash
# Copier tes fichiers CSV dans data/sources/
cp /chemin/vers/*.csv data/sources/
```

### 4 — Démarrer la stack

```bash
# Construire les images et démarrer tous les services
docker compose up --build -d

# Vérifier que tous les services sont Up
docker compose ps
```

### 5 — Créer les buckets MinIO

```bash
pip3 install minio --break-system-packages
python3 scripts/setup_minio.py
```

### 6 — Lancer le pipeline

```bash
# Déclencher manuellement le DAG Airflow
docker exec airflow-webserver airflow dags trigger academic_pipeline

# Ou depuis l'interface Airflow → http://<IP>:8080 → bouton ▶ Trigger DAG
```

### 7 — Démarrer le Thrift Server (pour Superset)

```bash
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

# Enregistrer les tables Gold
docker exec -i spark-master /opt/spark/bin/beeline \
  -u "jdbc:hive2://localhost:10000" -n "" -p "" << 'SQL'
CREATE TABLE IF NOT EXISTS gold_success_rate USING DELTA LOCATION 's3a://gold/success_rate/';
CREATE TABLE IF NOT EXISTS gold_student_performance USING DELTA LOCATION 's3a://gold/student_performance/';
CREATE TABLE IF NOT EXISTS gold_absenteeism USING DELTA LOCATION 's3a://gold/absenteeism/';
CREATE TABLE IF NOT EXISTS gold_program_kpis USING DELTA LOCATION 's3a://gold/program_kpis/';
CREATE TABLE IF NOT EXISTS gold_activity_summary USING DELTA LOCATION 's3a://gold/activity_summary/';
SHOW TABLES;
SQL
```

---

## Interfaces

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Apache Airflow** | `http://<IP>:8080` | admin / admin |
| **MinIO Console** | `http://<IP>:9001` | minioadmin / minioadmin123 |
| **Spark Web UI** | `http://<IP>:8082` | — |
| **Apache Superset** | `http://<IP>:8088` | admin / admin |

> Remplacer `<IP>` par l'adresse IP publique de la machine Oracle Cloud.

---

## Indicateurs analytiques

| Indicateur | Table Gold | Visualisation |
|------------|-----------|---------------|
| Taux de réussite par module | `gold_success_rate` | Bar chart par `module_name` |
| KPIs par filière et niveau | `gold_program_kpis` | Table avec `program_id`, `current_level` |
| Répartition des étudiants | `gold_student_performance` | Pie chart par `category` |
| Étudiants les plus absents | `gold_absenteeism` | Table triée par `total_hours_absent` |
| Engagement pédagogique | `gold_activity_summary` | Bar chart par `activity_type` et `platform` |

### Requêtes SQL de référence

```sql
-- KPIs par filière
SELECT program_id, current_level, success_rate_pct, nb_students, avg_grade
FROM gold_program_kpis
ORDER BY program_id, current_level;

-- Top 10 étudiants en difficulté
SELECT last_name, first_name, program_id, average_grade, total_hours_absent
FROM gold_student_performance
WHERE category = 'En difficulté'
ORDER BY average_grade ASC
LIMIT 10;

-- Modules avec le plus faible taux de réussite
SELECT module_name, program_id, difficulty, taux_reussite_pct, moyenne
FROM gold_success_rate
ORDER BY taux_reussite_pct ASC
LIMIT 10;

-- Corrélation absences / notes
SELECT s.program_id, s.average_grade, a.total_hours_absent, s.category
FROM gold_student_performance s
JOIN gold_absenteeism a ON s.student_id = a.student_id
WHERE a.total_hours_absent > 20
ORDER BY a.total_hours_absent DESC;
```

---

## Commandes utiles

### Gestion des conteneurs

```bash
docker compose ps                        # état de tous les services
docker compose logs -f <service>         # logs en temps réel
docker compose restart <service>         # redémarrer un service
docker compose down                      # arrêter (données conservées)
docker compose down -v                   # reset complet (supprime volumes)
docker compose up --build -d             # rebuild et relancer
```

### Tester les jobs Spark manuellement

```bash
# Job 1 : ingestion Bronze
docker exec airflow-webserver python3 /opt/spark/jobs/ingest_to_bronze.py

# Job 2 : nettoyage Silver
docker exec airflow-webserver python3 /opt/spark/jobs/bronze_to_silver.py

# Job 3 : agrégation Gold
docker exec airflow-webserver python3 /opt/spark/jobs/silver_to_gold.py
```

### Inspecter les données dans MinIO

```bash
# Vérifier les buckets
docker exec minio bash -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin123 && mc ls local/"

# Contenu du bucket gold
docker exec minio bash -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin123 && mc ls local/gold/"
```

### Surveiller les ressources

```bash
docker stats --no-stream    # RAM / CPU de chaque conteneur
df -h                       # espace disque disponible
```

---

## Contributeurs

| Nom | Filière | Rôle |
|-----|---------|------|
| — | II-BDCC | Développement pipeline Big Data |

---

*Projet réalisé dans le cadre du module Big Data — II-BDCC*  
*Encadrant : Prof. Abdelmajid BOUSSELHAM*
