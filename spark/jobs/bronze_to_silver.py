"""
Job 2 — Transformation : Bronze → Silver (Delta Lake)

Colonnes utilisées par table :
  students   : student_id, first_name, last_name, gender, email,
               program_id, cohort_year, current_level, registration_date
  programs   : program_id, program_name, department, responsible
  modules    : module_id, module_name, program_id, semester_id,
               hours, coefficient, difficulty
  semesters  : semester_id, academic_year, cohort_year, level, period
  grades     : grade_id, student_id, module_id, grade,
               session ("Normal"|"Rattrapage"), academic_year
  absences   : absence_id, student_id, module_id, absence_date,
               hours, justified ("Yes"|"No")
  activities : activity_id, student_id, module_id, activity_type,
               score, activity_date, platform
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")

from config import get_spark, BRONZE, SILVER
from pyspark.sql.functions import col, trim, upper, lower, to_date, when


def clean_students(spark):
    print("  → Nettoyage : students")
    df = spark.read.parquet(f"{BRONZE}/students/")
    silver = df \
        .dropDuplicates(["student_id"]) \
        .filter(col("student_id").isNotNull()) \
        .filter(col("program_id").isNotNull()) \
        .withColumn("first_name", trim(col("first_name"))) \
        .withColumn("last_name",  trim(upper(col("last_name")))) \
        .withColumn("gender",     upper(col("gender"))) \
        .withColumn("email",      lower(trim(col("email")))) \
        .withColumn("registration_date",
                    to_date(col("registration_date"), "yyyy-MM-dd"))
    silver.write.format("delta").mode("overwrite").save(f"{SILVER}/students/")
    print(f"     ✓ {silver.count()} étudiants")


def clean_programs(spark):
    print("  → Nettoyage : programs")
    df = spark.read.parquet(f"{BRONZE}/programs/")
    df.dropDuplicates(["program_id"]) \
      .write.format("delta").mode("overwrite").save(f"{SILVER}/programs/")
    print(f"     ✓ {df.count()} filières")


def clean_modules(spark):
    print("  → Nettoyage : modules")
    df = spark.read.parquet(f"{BRONZE}/modules/")
    df.dropDuplicates(["module_id"]) \
      .filter(col("module_id").isNotNull()) \
      .write.format("delta").mode("overwrite").save(f"{SILVER}/modules/")
    print(f"     ✓ {df.count()} modules")


def clean_semesters(spark):
    print("  → Nettoyage : semesters")
    df = spark.read.parquet(f"{BRONZE}/semesters/")
    df.dropDuplicates(["semester_id", "academic_year", "cohort_year"]) \
      .write.format("delta").mode("overwrite").save(f"{SILVER}/semesters/")
    print(f"     ✓ {df.count()} semestres")


def clean_grades(spark):
    """
    Notes valides entre 0 et 20.
    Ajout de : mention (Très Bien/Bien/Assez Bien/Passable/Insuffisant)
               is_passed (booléen)
    """
    print("  → Nettoyage : grades")
    df = spark.read.parquet(f"{BRONZE}/grades/")
    silver = df \
        .dropDuplicates(["grade_id"]) \
        .filter(col("grade").isNotNull()) \
        .filter((col("grade") >= 0) & (col("grade") <= 20)) \
        .withColumn("mention",
            when(col("grade") >= 16, "Très Bien")
            .when(col("grade") >= 14, "Bien")
            .when(col("grade") >= 12, "Assez Bien")
            .when(col("grade") >= 10, "Passable")
            .otherwise("Insuffisant")
        ) \
        .withColumn("is_passed", col("grade") >= 10)
    silver.write.format("delta").mode("overwrite").save(f"{SILVER}/grades/")
    print(f"     ✓ {silver.count()} notes")


def clean_absences(spark):
    """
    justified est une chaîne "Yes"/"No" dans le CSV.
    Ajout de is_justified (booléen) pour faciliter les agrégations.
    """
    print("  → Nettoyage : absences")
    df = spark.read.parquet(f"{BRONZE}/absences/")
    silver = df \
        .dropDuplicates(["absence_id"]) \
        .filter(col("hours") > 0) \
        .withColumn("absence_date",
                    to_date(col("absence_date"), "yyyy-MM-dd")) \
        .withColumn("is_justified",
                    when(upper(col("justified")) == "YES", True)
                    .otherwise(False))
    silver.write.format("delta").mode("overwrite").save(f"{SILVER}/absences/")
    print(f"     ✓ {silver.count()} absences")


def clean_activities(spark):
    """
    Scores valides entre 0 et 20.
    Types : Participation, Quiz, Assignment, Project, PFE Report,
            PFE Presentation, PFE Progress
    Plateformes : Teams, Google Classroom, Moodle
    """
    print("  → Nettoyage : activities")
    df = spark.read.parquet(f"{BRONZE}/activities/")
    silver = df \
        .dropDuplicates(["activity_id"]) \
        .filter(col("score").isNotNull()) \
        .filter((col("score") >= 0) & (col("score") <= 20)) \
        .withColumn("activity_date",
                    to_date(col("activity_date"), "yyyy-MM-dd"))
    silver.write.format("delta").mode("overwrite").save(f"{SILVER}/activities/")
    print(f"     ✓ {silver.count()} activités")


def main():
    spark = get_spark("Bronze_to_Silver")
    spark.sparkContext.setLogLevel("WARN")

    print("=== Démarrage : Transformation Silver ===\n")
    clean_students(spark)
    clean_programs(spark)
    clean_modules(spark)
    clean_semesters(spark)
    clean_grades(spark)
    clean_absences(spark)
    clean_activities(spark)

    print("\n✅ Transformation Silver terminée — 7 tables Delta Lake")
    spark.stop()


if __name__ == "__main__":
    main()
