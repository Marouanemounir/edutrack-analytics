"""
Job 3 — Agrégation : Silver → Gold (Delta Lake)

5 tables analytiques produites :
  gold_success_rate       : taux de réussite par module
  gold_absenteeism        : résumé absences par étudiant
  gold_student_performance: performance individuelle (notes + absences)
  gold_program_kpis       : KPIs par filière et niveau
  gold_activity_summary   : engagement par type d'activité et plateforme
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")

from config import get_spark, SILVER, GOLD
from pyspark.sql.functions import (
    col, avg, count, sum as _sum,
    round as spark_round, when,
    max as spark_max, min as spark_min, stddev
)


def gold_success_rate(spark):
    """
    Taux de réussite, moyenne, écart-type par module.
    Jointure grades ↔ modules sur module_id.
    Filière = program_id (BDCC | CCN | GLSID).
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
            spark_round(avg("grade"),        2).alias("moyenne"),
            spark_round(spark_max("grade"),  2).alias("grade_max"),
            spark_round(spark_min("grade"),  2).alias("grade_min"),
            spark_round(stddev("grade"),     2).alias("ecart_type"),
            spark_round(
                count(when(col("grade") >= 10, True)) / count("*") * 100, 1
            ).alias("taux_reussite_pct")
        )
    df.write.format("delta").mode("overwrite").save(f"{GOLD}/success_rate/")
    print(f"     ✓ {df.count()} lignes")


def gold_absenteeism(spark):
    """
    Heures d'absence totales par étudiant.
    Jointure absences ↔ students sur student_id.
    is_justified est un booléen ajouté dans Silver.
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
    df.write.format("delta").mode("overwrite").save(f"{GOLD}/absenteeism/")
    print(f"     ✓ {df.count()} lignes")


def gold_student_performance(spark):
    """
    Vue synthétique par étudiant : moyenne + absences + catégorie.
    session == "Normal" = session ordinaire (pas rattrapage).
    """
    print("  → Calcul : gold_student_performance")
    grades   = spark.read.format("delta").load(f"{SILVER}/grades/")
    absences = spark.read.format("delta").load(f"{SILVER}/absences/")
    students = spark.read.format("delta").load(f"{SILVER}/students/")

    # Notes — session normale uniquement
    perf = grades \
        .filter(col("session") == "Normal") \
        .groupBy("student_id") \
        .agg(
            spark_round(avg("grade"), 2).alias("average_grade"),
            count("*").alias("nb_modules_enrolled"),
            count(when(col("grade") >= 10, True)).alias("nb_modules_passed"),
        )

    # Absences
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
    df.write.format("delta").mode("overwrite").save(f"{GOLD}/student_performance/")
    print(f"     ✓ {df.count()} lignes")


def gold_program_kpis(spark):
    """
    KPIs agrégés par filière (program_id) et niveau (current_level).
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
    df.write.format("delta").mode("overwrite").save(f"{GOLD}/program_kpis/")
    print(f"     ✓ {df.count()} lignes")


def gold_activity_summary(spark):
    """
    Engagement pédagogique par type d'activité et plateforme.
    Utilise la table activities (16 577 lignes).
    """
    print("  → Calcul : gold_activity_summary")
    activities = spark.read.format("delta").load(f"{SILVER}/activities/")

    df = activities \
        .groupBy("activity_type", "platform", "academic_year") \
        .agg(
            count("*").alias("nb_activities"),
            spark_round(avg("score"),        2).alias("avg_score"),
            spark_round(spark_max("score"),  2).alias("max_score"),
            spark_round(spark_min("score"),  2).alias("min_score"),
            spark_round(
                count(when(col("score") >= 10, True)) / count("*") * 100, 1
            ).alias("success_rate_pct")
        )
    df.write.format("delta").mode("overwrite").save(f"{GOLD}/activity_summary/")
    print(f"     ✓ {df.count()} lignes")


def main():
    spark = get_spark("Silver_to_Gold")
    spark.sparkContext.setLogLevel("WARN")

    print("=== Démarrage : Agrégation Gold ===\n")
    gold_success_rate(spark)
    gold_absenteeism(spark)
    gold_student_performance(spark)
    gold_program_kpis(spark)
    gold_activity_summary(spark)

    print("\n✅ Agrégation Gold terminée — 5 tables analytiques produites")
    spark.stop()


if __name__ == "__main__":
    main()
