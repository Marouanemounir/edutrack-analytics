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
- [Delta Lake — Fonctionnalités avancées](#delta-lake--fonctionnalités-avancées)
- [Commandes utiles](#commandes-utiles)

---

## Présentation

**EduTrack Analytics** est une plateforme Big Data de type **Lakehouse** conçue pour centraliser, transformer et analyser les données académiques de trois filières : **BDCC**, **CCN** et **GLSID**.

Elle permet de calculer des indicateurs de pilotage pédagogique tels que :
- le taux de réussite par module et par filière
- le suivi de l'absentéisme étudiant
- la corrélation entre absences et résultats
- la performance individuelle et collective
- l'engagement sur les activités pédagogiques
- le pilotage par semestre et année universitaire

Le pipeline est **entièrement orchestré par Apache Airflow** et s'exécute automatiquement chaque nuit en mode batch.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           SOURCES DE DONNÉES                                 │
│                                                                              │
│  Source 1 — Fichiers CSV          Source 2 — PostgreSQL (ERP scolarité)     │
│  students · grades · absences     registrations (inscriptions, statuts)     │
│  activities · modules · programs                                             │
│  semesters                                                                   │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION — Apache Airflow                        │
│                                                                              │
│  start ──► ingest_to_bronze ──► bronze_to_silver ──► silver_to_gold ──► end │
│                                                                              │
│  Planification : quotidienne à 02h00  (cron: 0 2 * * *)                     │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
         ┌─────────┐    ┌──────────┐    ┌──────────┐
         │  BRONZE │    │  SILVER  │    │   GOLD   │
         │ Parquet │───►│  Delta   │───►│  Delta   │
         │  brut   │    │ nettoyé  │    │  KPIs    │
         │ 8 tables│    │ 8 tables │    │ 7 tables │
         └─────────┘    └──────────┘    └──────────┘
                                               │
                               ┌───────────────▼───────────────┐
                               │            MinIO              │
                               │   Stockage objet S3-compat.   │
                               │   buckets: bronze/silver/gold │
                               └───────────────┬───────────────┘
                                               │
                               ┌───────────────▼───────────────┐
                               │      Spark Thrift Server      │
                               │         (JDBC / SQL)          │
                               └───────────────┬───────────────┘
                                               │
                               ┌───────────────▼───────────────┐
                               │        Apache Superset        │
                               │   Dashboards de pilotage      │
                               └───────────────────────────────┘
```

---

## Stack technique

| Technologie | Version | Rôle |
|-------------|---------|------|
| **Apache Airflow** | 2.9.0 | Orchestration et planification des traitements batch |
| **Apache Spark** | 3.5.0 | Traitement Big Data batch — ETL Bronze → Silver → Gold |
| **Delta Lake** | 3.2.0 | Format transactionnel pour les tables analytiques |
| **MinIO** | latest | Stockage objet S3-compatible (couche physique du Lakehouse) |
| **Spark Thrift Server** | 3.5.0 | Exposition SQL des tables Delta via JDBC/ODBC |
| **Apache Superset** | 3.1.0 | Visualisation et tableaux de bord analytiques |
| **PostgreSQL** | 15 | Base de métadonnées Airflow + source ERP simulée |
| **Docker Compose** | v2 | Déploiement unifié de toute la plateforme |

---

## Structure du projet

```
projet-bigdata/
├── .env                            ← Variables secrètes (non versionné)
├── .gitignore
├── docker-compose.yml              ← Définition des 7 services
├── README.md
│
├── airflow/
│   ├── Dockerfile                  ← Image Airflow + Java 17 + PySpark
│   └── dags/
│       └── academic_pipeline.py   ← DAG principal d'orchestration
│
├── spark/
│   ├── Dockerfile                  ← Image Spark + Delta Lake JARs + PostgreSQL JDBC
│   └── jobs/
│       ├── config.py               ← Configuration Spark partagée (MinIO, Delta)
│       ├── ingest_to_bronze.py     ← Job 1 : multi-sources → Parquet Bronze
│       ├── bronze_to_silver.py     ← Job 2 : nettoyage → Delta Lake Silver
│       └── silver_to_gold.py       ← Job 3 : 7 tables analytiques → Delta Lake Gold
│
├── data/
│   └── sources/                    ← 7 fichiers CSV sources
│       ├── students.csv
│       ├── programs.csv
│       ├── modules.csv
│       ├── semesters.csv
│       ├── grades.csv
│       ├── absences.csv
│       └── activities.csv
│
└── scripts/
    ├── setup_minio.py              ← Création des 3 buckets MinIO
    └── start_thrift.sh             ← Démarrage Thrift Server + enregistrement tables
```

---

## Données

### Sources ingérées

| Source | Fichier / Table | Lignes | Description |
|--------|----------------|--------|-------------|
| **CSV** | `students.csv` | 358 | Étudiants (BDCC, CCN, GLSID) — 3 cohortes 2023-2026 |
| **CSV** | `programs.csv` | 3 | Filières avec responsable et département |
| **CSV** | `modules.csv` | 78 | Modules avec coefficient, semestre et difficulté |
| **CSV** | `semesters.csv` | 12 | Semestres S1–S6 sur 3 années universitaires |
| **CSV** | `grades.csv` | 6 686 | Notes session Normal / Rattrapage, entre 0 et 20 |
| **CSV** | `absences.csv` | 9 643 | Absences avec heures et justification (Yes/No) |
| **CSV** | `activities.csv` | 16 577 | Quiz, Assignment, Project, PFE — 3 plateformes |
| **PostgreSQL** | `registrations` | 358 | Inscriptions ERP — statuts et paiements scolarité |

**Total : ~34 000 enregistrements — 2 sources hétérogènes**

---

## Pipeline de données

### Couche Bronze — Données brutes (Parquet)

Ingestion directe depuis 2 sources sans transformation. Format Parquet.

```
s3a://bronze/students/        s3a://bronze/programs/
s3a://bronze/modules/         s3a://bronze/semesters/
s3a://bronze/grades/          s3a://bronze/absences/
s3a://bronze/activities/      s3a://bronze/registrations/   ← source PostgreSQL
```

### Couche Silver — Données nettoyées (Delta Lake)

Transformations appliquées :
- Suppression des doublons (sur clés primaires)
- Normalisation des formats de dates (`yyyy-MM-dd`)
- Normalisation des chaînes (trim, upper/lower)
- Filtrage des valeurs aberrantes (notes hors [0, 20])
- Conversion `justified` : `"Yes"/"No"` → booléen `is_justified`
- Ajout de colonnes calculées : `mention`, `is_passed`
- Partitionnement Delta Lake par `program_id`

```
s3a://silver/students/        s3a://silver/programs/
s3a://silver/modules/         s3a://silver/semesters/
s3a://silver/grades/          s3a://silver/absences/
s3a://silver/activities/      s3a://silver/registrations/
```

### Couche Gold — Tables analytiques (Delta Lake)

| Table | Description | Indicateur couvert |
|-------|-------------|-------------------|
| `gold_success_rate` | Taux de réussite, moyenne, écart-type par module | Taux de réussite par module |
| `gold_absenteeism` | Total heures d'absence par étudiant | Nombre d'absences par étudiant |
| `gold_student_performance` | Moyenne + catégorie (Excellent/Bon/Passable/En difficulté) | Suivi étudiants en difficulté |
| `gold_program_kpis` | KPIs agrégés par filière et niveau | Taux de réussite par filière |
| `gold_activity_summary` | Engagement par type d'activité et plateforme | Activités pédagogiques |
| `gold_correlation_abs_grades` | Corrélation absences / notes par catégorie | Corrélation absences / résultats |
| `gold_semester_dashboard` | Évolution des performances par semestre | Pilotage par semestre |

```
s3a://gold/success_rate/           s3a://gold/absenteeism/
s3a://gold/student_performance/    s3a://gold/program_kpis/
s3a://gold/activity_summary/       s3a://gold/correlation_abs_grades/
s3a://gold/semester_dashboard/
```

---

## Lancement rapide

### Prérequis

- Machine Ubuntu (Oracle Cloud) avec **8 GB RAM minimum**
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
nano .env   # Remplir FERNET_KEY, SECRET_KEY, SUPERSET_SECRET_KEY
```

Générer les clés :
```bash
python3 -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())"
python3 -c "import secrets; print('SECRET_KEY=' + secrets.token_hex(32))"
python3 -c "import secrets; print('SUPERSET_SECRET_KEY=' + secrets.token_hex(32))"
```

### 3 — Copier les fichiers CSV

```bash
# Depuis la machine locale
scp -i cle.key *.csv ubuntu@<IP_VM>:~/edutrack-analytics/data/sources/
```

### 4 — Démarrer la stack

```bash
docker compose up --build -d
docker compose ps   # vérifier que les 7 services sont Up
```

### 5 — Créer les buckets MinIO

```bash
pip3 install minio --break-system-packages
python3 scripts/setup_minio.py
```

### 6 — Créer la source PostgreSQL (2ème source)

```bash
docker exec -i postgres psql -U airflow -d airflow << 'SQL'
CREATE TABLE IF NOT EXISTS registrations (
    registration_id SERIAL PRIMARY KEY,
    student_id VARCHAR(20), program_id VARCHAR(10),
    academic_year VARCHAR(10), registration_date DATE,
    status VARCHAR(20), tuition_paid BOOLEAN
);
INSERT INTO registrations (student_id, program_id, academic_year, registration_date, status, tuition_paid)
SELECT 'STU-' || LPAD(gs::text, 4, '0'),
    (ARRAY['BDCC','CCN','GLSID'])[1 + (gs % 3)], '2023-2024',
    DATE '2023-09-01' + (gs % 30),
    (ARRAY['Inscrit','Redoublant','Transfert'])[1 + (gs % 3)], (gs % 2 = 0)
FROM generate_series(1, 358) gs;
SQL
```

### 7 — Lancer le pipeline

```bash
# Via Airflow CLI
docker exec airflow-webserver airflow dags trigger academic_pipeline

# Ou via l'interface → http://<IP>:8080 → bouton ▶ Trigger DAG
```

Toutes les tâches doivent passer au vert :
```
start ✅ → ingest_to_bronze ✅ → bronze_to_silver ✅ → silver_to_gold ✅ → end ✅
```

### 8 — Démarrer le Thrift Server

```bash
bash scripts/start_thrift.sh
```

Ou manuellement :
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

# Attendre que le port soit ouvert
until docker exec spark-master netstat -tlnp 2>/dev/null | grep -q 10000; do sleep 5; done

# Enregistrer les 7 tables Gold
docker exec -i spark-master /opt/spark/bin/beeline \
  -u "jdbc:hive2://localhost:10000" -n "" -p "" << 'SQL'
CREATE TABLE IF NOT EXISTS gold_success_rate USING DELTA LOCATION 's3a://gold/success_rate/';
CREATE TABLE IF NOT EXISTS gold_absenteeism USING DELTA LOCATION 's3a://gold/absenteeism/';
CREATE TABLE IF NOT EXISTS gold_student_performance USING DELTA LOCATION 's3a://gold/student_performance/';
CREATE TABLE IF NOT EXISTS gold_program_kpis USING DELTA LOCATION 's3a://gold/program_kpis/';
CREATE TABLE IF NOT EXISTS gold_activity_summary USING DELTA LOCATION 's3a://gold/activity_summary/';
CREATE TABLE IF NOT EXISTS gold_correlation_abs_grades USING DELTA LOCATION 's3a://gold/correlation_abs_grades/';
CREATE TABLE IF NOT EXISTS gold_semester_dashboard USING DELTA LOCATION 's3a://gold/semester_dashboard/';
SHOW TABLES;
SQL
```

### 9 — Configurer Superset

1. Ouvre `http://<IP>:8088` → `admin / admin`
2. **Settings → Database Connections → + Database → Apache Hive**
3. URI : `hive://spark-master:10000/default`
4. **Test Connection → Connect**
5. **Datasets → + Dataset** : créer les 7 datasets via SQL :
   ```sql
   SELECT * FROM delta.`s3a://gold/program_kpis/`
   -- (répéter pour chaque table Gold)
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

| Indicateur | Table Gold | Type de graphique |
|------------|-----------|-------------------|
| Taux de réussite par module | `gold_success_rate` | Bar Chart — `module_name` / `taux_reussite_pct` |
| KPIs par filière et niveau | `gold_program_kpis` | Table — `program_id`, `avg_grade`, `success_rate_pct` |
| Répartition des étudiants | `gold_student_performance` | Pie Chart — `category` |
| Top étudiants absents | `gold_absenteeism` | Table — `total_hours_absent` DESC |
| Engagement pédagogique | `gold_activity_summary` | Bar Chart — `activity_type` / `platform` |
| Corrélation absences / notes | `gold_correlation_abs_grades` | Bar Chart — `absence_category` / `avg_grade` |
| Évolution par semestre | `gold_semester_dashboard` | Line Chart — `semester_id` / `success_rate_pct` |

### Requêtes SQL analytiques de référence

```sql
-- KPIs par filière
SELECT program_id, current_level, success_rate_pct, nb_students, avg_grade, nb_at_risk
FROM gold_program_kpis
ORDER BY program_id, current_level;

-- Top 10 étudiants en difficulté
SELECT last_name, first_name, program_id, average_grade, total_hours_absent
FROM gold_student_performance
WHERE category = 'En difficulté'
ORDER BY average_grade ASC
LIMIT 10;

-- Corrélation absences / notes
SELECT absence_category, program_id, nb_students, avg_grade, success_rate_pct
FROM gold_correlation_abs_grades
ORDER BY program_id, absence_category;

-- Évolution par semestre
SELECT semester_id, program_id, success_rate_pct, nb_students_evaluated
FROM gold_semester_dashboard
ORDER BY program_id, semester_id;

-- Modules les plus difficiles
SELECT module_name, program_id, difficulty, taux_reussite_pct, moyenne, ecart_type
FROM gold_success_rate
ORDER BY taux_reussite_pct ASC
LIMIT 10;
```

---

## Delta Lake — Fonctionnalités avancées

Se connecter à Beeline :
```bash
docker exec -it spark-master /opt/spark/bin/beeline \
  -u "jdbc:hive2://localhost:10000" -n "" -p ""
```

### Time Travel — Interroger une version antérieure

```sql
-- Historique des versions d'une table
DESCRIBE HISTORY gold_program_kpis;

-- Lire la version initiale (version 0)
SELECT * FROM delta.`s3a://gold/program_kpis/` VERSION AS OF 0;
```

### OPTIMIZE — Compacter les petits fichiers

```sql
-- Compacter et Z-order sur la colonne la plus filtrée
OPTIMIZE delta.`s3a://gold/student_performance/`
  ZORDER BY (program_id);
```

### VACUUM — Nettoyer les anciennes versions

```sql
-- Supprimer les fichiers de plus de 7 jours
VACUUM delta.`s3a://gold/student_performance/` RETAIN 168 HOURS;
```

### Détails de la table

```sql
-- Schéma et métadonnées
DESCRIBE DETAIL delta.`s3a://gold/success_rate/`;
```

---

## Commandes utiles

### Gestion de la stack

```bash
docker compose ps                        # état de tous les services
docker compose logs -f <service>         # logs en temps réel
docker compose restart <service>         # redémarrer un service
docker compose down                      # arrêter (données MinIO conservées)
docker compose down -v                   # reset complet (supprime volumes)
docker compose up --build -d             # rebuild et relancer
```

### Tester les jobs Spark manuellement

```bash
docker exec airflow-webserver python3 /opt/spark/jobs/ingest_to_bronze.py
docker exec airflow-webserver python3 /opt/spark/jobs/bronze_to_silver.py
docker exec airflow-webserver python3 /opt/spark/jobs/silver_to_gold.py
```

### Inspecter MinIO

```bash
# Lister les buckets
docker exec minio bash -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin123 && mc ls local/"

# Contenu du bucket gold (7 dossiers attendus)
docker exec minio bash -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin123 && mc ls local/gold/"
```

### Surveiller les ressources

```bash
docker stats --no-stream    # RAM / CPU de chaque conteneur
df -h                       # espace disque disponible
du -sh ~/projet-bigdata/    # taille totale du projet
```

### Checklist avant soutenance

- [ ] Les 7 fichiers CSV sont dans `data/sources/`
- [ ] Tous les services Docker sont `Up` (`docker compose ps`)
- [ ] Les 3 buckets MinIO existent : bronze, silver, gold
- [ ] La table `registrations` existe dans PostgreSQL
- [ ] Le DAG Airflow : 4 tâches ✅ vertes
- [ ] Bronze : 8 dossiers Parquet dans MinIO
- [ ] Silver : 8 tables Delta Lake
- [ ] Gold : 7 tables analytiques avec partitionnement
- [ ] Spark Thrift Server répond sur le port 10000
- [ ] 7 datasets créés dans Superset
- [ ] Dashboard Superset avec 7 graphiques et filtre `program_id`
- [ ] Ports Oracle Cloud ouverts (8080, 8082, 8088, 9000, 9001)
- [ ] README.md à jour sur GitHub

---

## Contributeurs

| Nom | Filière | Rôle |
|-----|---------|------|
| — | II-BDCC | Développement pipeline Big Data |

---

*Projet réalisé dans le cadre du module Big Data — II-BDCC*
*Encadrant : Prof. Abdelmajid BOUSSELHAM*
*Données : 3 filières (BDCC · CCN · GLSID) · 358 étudiants · 3 cohortes (2023–2026)*