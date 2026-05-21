"""
Job optionnel — Contrôle qualité des données Bronze
Vérifie : nulls critiques, doublons, valeurs hors plage
"""
import sys
sys.path.insert(0, "/opt/spark/jobs")
from config import get_spark, BRONZE


def check_table(spark, table, pk, checks):
    df = spark.read.parquet(f"{BRONZE}/{table}/")
    total = df.count()
    results = {"table": table, "total_rows": total}

    # Doublons sur la clé primaire
    dupes = total - df.dropDuplicates([pk]).count()
    results["duplicates"] = dupes

    # Nulls sur colonnes critiques
    for col_name in checks.get("not_null", []):
        nulls = df.filter(df[col_name].isNull()).count()
        results[f"nulls_{col_name}"] = nulls

    # Valeurs hors plage
    for col_name, (min_val, max_val) in checks.get("range", {}).items():
        from pyspark.sql.functions import col
        out = df.filter(
            (col(col_name) < min_val) | (col(col_name) > max_val)
        ).count()
        results[f"out_of_range_{col_name}"] = out

    status = "✅ OK" if dupes == 0 and all(
        v == 0 for k, v in results.items() if k.startswith("nulls_")
    ) else "⚠️  ISSUES"
    print(f"  {status} {table:15s} | {total:6d} lignes | {results}")
    return results


def main():
    spark = get_spark("Quality_Check")
    spark.sparkContext.setLogLevel("WARN")

    print("=== Contrôle Qualité Bronze ===\n")
    check_table(spark, "students", "student_id",
        {"not_null": ["student_id", "program_id", "current_level"]})
    check_table(spark, "grades", "grade_id",
        {"not_null": ["student_id", "module_id", "grade"],
         "range": {"grade": (0, 20)}})
    check_table(spark, "absences", "absence_id",
        {"not_null": ["student_id", "hours"],
         "range": {"hours": (0, 100)}})
    check_table(spark, "activities", "activity_id",
        {"not_null": ["student_id", "score"],
         "range": {"score": (0, 20)}})

    print("\n✅ Contrôle qualité terminé")
    spark.stop()


if __name__ == "__main__":
    main()
