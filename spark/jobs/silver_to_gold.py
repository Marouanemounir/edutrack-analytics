"""
Job 3 — Agrégation : Silver → Gold (Delta Lake)

6 tables analytiques produites :
  1. gold_success_rate          : taux de réussite par module
  2. gold_absenteeism           : résumé absences par étudiant
  3. gold_student_performance   : performance individuelle
  4. gold_program_kpis          : KPIs par filière et niveau
  5. gold_activity_summary      : engagement par activité et plateforme
  6. gold_correlation_abs_grades: corrélation absences / résultats
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")

from config import get_spark, SILVER, GOLD
from pyspark.sql.functions import (
    col, avg, count, sum as _sum,
    round as spark_round, when,
    max as spark_max, min as spark_min,
    stddev, countDistinct
)


def gold_success_rate(spark):
    """
    Taux de réussite, moyenne, écart-type par module.
    Jointure grades ↔ modules sur module_id.
    """
    print("  → Calcul : gold_success_rate")
    grades  = spark.read.format("delta").load(f"{SILVER}/grades/")
    modules = spark.read.format("delta").load(f"{SILVER}/modules/")

    df = grades \
        .join(modules, "module_id") \
        .groupBy("module_id", "module_name", "program_id",
                 "difficulty", "coefficient", "academic_year") \
        .agg(
            count("*").alias("nb_grades"),
            spark_round(avg("grade"),       2).alias("moyenne"),
            spark_round(spark_max("grade"), 2).alias("grade_max"),
            spark_round(spark_min("grade"), 2).alias("grade_min"),
            spark_round(stddev("grade"),    2).alias("ecart_type"),
            spark_round(
                count(when(col("grade") >= 10, True)) / count("*") * 100, 1
            ).alias("taux_reussite_pct")
        )

    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .partitionBy("program_id") \
      .save(f"{GOLD}/success_rate/")
    print(f"     ✓ {df.count()} lignes écrites")


def gold_absenteeism(spark):
    """
    Heures d'absence totales par étudiant.
    is_justified est un booléen ajouté dans Silver (Yes/No → True/False).
    """
    print("  → Calcul : gold_absenteeism")
    absences = spark.read.format("delta").load(f"{SILVER}/absences/")
    students = spark.read.format("delta").load(f"{SILVER}/students/")

    df = absences \
        .join(students, "student_id") \
        .groupBy("student_id", "first_name", "last_name",
                 "program_id", "current_level", "academic_year") \
        .agg(
            _sum("hours").alias("total_hours_absent"),
            count("*").alias("nb_absences"),
            count(when(col("is_justified") == True,  True)).alias("nb_justified"),
            count(when(col("is_justified") == False, True)).alias("nb_unjustified"),
        ) \
        .withColumn("justification_rate_pct",
            spark_round(col("nb_justified") / col("nb_absences") * 100, 1)
        )

    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .partitionBy("program_id") \
      .save(f"{GOLD}/absenteeism/")
    print(f"     ✓ {df.count()} lignes écrites")


def gold_student_performance(spark):
    """
    Vue synthétique par étudiant : moyenne + absences + catégorie.
    Filtre sur session == "Normal" (session ordinaire uniquement).
    """
    print("  → Calcul : gold_student_performance")
    grades   = spark.read.format("delta").load(f"{SILVER}/grades/")
    absences = spark.read.format("delta").load(f"{SILVER}/absences/")
    students = spark.read.format("delta").load(f"{SILVER}/students/")

    perf = grades \
        .filter(col("session") == "Normal") \
        .groupBy("student_id") \
        .agg(
            spark_round(avg("grade"), 2).alias("average_grade"),
            count("*").alias("nb_modules_enrolled"),
            count(when(col("grade") >= 10, True)).alias("nb_modules_passed"),
        )

    abs_summary = absences \
        .groupBy("student_id") \
        .agg(_sum("hours").alias("total_hours_absent"))

    df = students \
        .join(perf,        "student_id", "left") \
        .join(abs_summary, "student_id", "left") \
        .withColumn("total_hours_absent",
            when(col("total_hours_absent").isNull(), 0)
            .otherwise(col("total_hours_absent"))
        ) \
        .withColumn("category",
            when(col("average_grade") >= 14, "Excellent")
            .when(col("average_grade") >= 12, "Bon")
            .when(col("average_grade") >= 10, "Passable")
            .otherwise("En difficulté")
        )

    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .partitionBy("program_id") \
      .save(f"{GOLD}/student_performance/")
    print(f"     ✓ {df.count()} lignes écrites")


def gold_program_kpis(spark):
    """
    KPIs agrégés par filière et niveau.
    program_id : BDCC | CCN | GLSID
    current_level : 1 | 2 | 3
    """
    print("  → Calcul : gold_program_kpis")
    perf = spark.read.format("delta").load(f"{GOLD}/student_performance/")

    df = perf \
        .groupBy("program_id", "current_level") \
        .agg(
            count("*").alias("nb_students"),
            spark_round(avg("average_grade"),      2).alias("avg_grade"),
            spark_round(
                count(when(col("average_grade") >= 10, True)) / count("*") * 100, 1
            ).alias("success_rate_pct"),
            spark_round(avg("total_hours_absent"), 1).alias("avg_hours_absent"),
            count(when(col("category") == "En difficulté", True))
             .alias("nb_at_risk"),
        )

    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .save(f"{GOLD}/program_kpis/")
    print(f"     ✓ {df.count()} lignes écrites")


def gold_activity_summary(spark):
    """
    Engagement pédagogique par type d'activité, plateforme et filière.

    Types d'activité : Participation, Quiz, Assignment,
                       Project, PFE Report, PFE Presentation, PFE Progress
    Plateformes      : Teams, Google Classroom, Moodle

    Jointure activities ↔ students pour obtenir program_id.
    """
    print("  → Calcul : gold_activity_summary")
    activities = spark.read.format("delta").load(f"{SILVER}/activities/")
    students   = spark.read.format("delta").load(f"{SILVER}/students/")

    df = activities \
        .join(students.select("student_id", "program_id", "current_level"),
              "student_id") \
        .groupBy("activity_type", "platform", "program_id", "academic_year") \
        .agg(
            count("*").alias("nb_activities"),
            countDistinct("student_id").alias("nb_students"),
            spark_round(avg("score"),        2).alias("avg_score"),
            spark_round(spark_max("score"),  2).alias("max_score"),
            spark_round(spark_min("score"),  2).alias("min_score"),
            spark_round(stddev("score"),     2).alias("ecart_type"),
            spark_round(
                count(when(col("score") >= 10, True)) / count("*") * 100, 1
            ).alias("success_rate_pct")
        )

    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .partitionBy("program_id") \
      .save(f"{GOLD}/activity_summary/")
    print(f"     ✓ {df.count()} lignes écrites")


def gold_correlation_abs_grades(spark):
    """
    Corrélation entre heures d'absence et moyenne des notes.
    """
    print("  → Calcul : gold_correlation_abs_grades")
    perf     = spark.read.format("delta").load(f"{GOLD}/student_performance/")
    abs_data = spark.read.format("delta").load(f"{GOLD}/absenteeism/")

    # Sélectionner uniquement les colonnes nécessaires depuis abs_data
    # pour éviter le conflit sur total_hours_absent
    abs_selected = abs_data.select(
        "student_id",
        col("total_hours_absent").alias("hours_absent")
    )

    df = perf \
        .join(abs_selected, "student_id", "left") \
        .withColumn("absence_category",
            when(col("hours_absent") <= 5,  "Très faible (0-5h)")
            .when(col("hours_absent") <= 15, "Faible (6-15h)")
            .when(col("hours_absent") <= 30, "Modéré (16-30h)")
            .otherwise("Élevé (>30h)")
        ) \
        .groupBy("absence_category", "program_id") \
        .agg(
            count("*").alias("nb_students"),
            spark_round(avg("average_grade"),  2).alias("avg_grade"),
            spark_round(
                count(when(col("average_grade") >= 10, True)) / count("*") * 100, 1
            ).alias("success_rate_pct"),
            spark_round(avg("hours_absent"), 1).alias("avg_hours_absent")
        ) \
        .orderBy("program_id", "absence_category")

    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .save(f"{GOLD}/correlation_abs_grades/")
    print(f"     ✓ {df.count()} lignes écrites")


def gold_semester_dashboard(spark):
    """
    Tableau de bord par semestre et année universitaire.
    """
    print("  → Calcul : gold_semester_dashboard")
    grades   = spark.read.format("delta").load(f"{SILVER}/grades/")
    absences = spark.read.format("delta").load(f"{SILVER}/absences/")
    modules  = spark.read.format("delta").load(f"{SILVER}/modules/")

    # Renommer semester_id dans modules pour éviter l'ambiguïté
    modules_slim = modules.select(
        col("module_id"),
        col("program_id"),
        col("semester_id").alias("module_semester_id")
    )

    # Joindre grades avec modules — utiliser module_semester_id
    grades_enriched = grades \
        .join(modules_slim, "module_id") \
        .select(
            col("student_id"),
            col("grade"),
            col("academic_year"),
            col("cohort_year"),
            col("program_id"),
            col("module_semester_id").alias("semester_id")
        )

    # Agréger par semestre
    sem_grades = grades_enriched \
        .groupBy("semester_id", "program_id", "academic_year", "cohort_year") \
        .agg(
            count("*").alias("nb_grades"),
            spark_round(avg("grade"), 2).alias("avg_grade"),
            spark_round(
                count(when(col("grade") >= 10, True)) / count("*") * 100, 1
            ).alias("success_rate_pct"),
            countDistinct("student_id").alias("nb_students_evaluated")
        )

    # Agréger absences par semestre — renommer pour éviter conflit
    sem_abs = absences \
        .select(
            col("semester_id").alias("abs_semester_id"),
            col("academic_year").alias("abs_academic_year"),
            col("hours"),
            col("is_justified")
        ) \
        .groupBy("abs_semester_id", "abs_academic_year") \
        .agg(
            _sum("hours").alias("total_hours_absent"),
            spark_round(avg("hours"), 2).alias("avg_hours_per_absence"),
            count(when(col("is_justified") == False, True))
             .alias("nb_unjustified")
        )

    # Jointure finale sans ambiguïté
    df = sem_grades.join(
        sem_abs,
        (sem_grades["semester_id"]   == sem_abs["abs_semester_id"]) &
        (sem_grades["academic_year"] == sem_abs["abs_academic_year"]),
        "left"
    ).drop("abs_semester_id", "abs_academic_year")

    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .save(f"{GOLD}/semester_dashboard/")
    print(f"     ✓ {df.count()} lignes écrites")


def main():
    spark = get_spark("Silver_to_Gold")
    spark.sparkContext.setLogLevel("WARN")

    print("=== Démarrage : Agrégation Gold ===\n")
    gold_success_rate(spark)
    gold_absenteeism(spark)
    gold_student_performance(spark)
    gold_program_kpis(spark)
    gold_activity_summary(spark)
    gold_correlation_abs_grades(spark)
    gold_semester_dashboard(spark)        # ← ajouter ici

    print("\n✅ Agrégation Gold terminée — 7 tables analytiques produites")
    spark.stop()


if __name__ == "__main__":
    main()
