#!/usr/bin/env python3
"""
PySpark-задание для обработки заявок на кредит.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # Читаем аргументы напрямую из sys.argv
    print(f"[INFO] sys.argv: {sys.argv}")

    input_path  = sys.argv[1] if len(sys.argv) > 1 else None
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    report_date = sys.argv[3] if len(sys.argv) > 3 else "2026-06-16"

    # Проверяем что пути начинаются с s3a://
    assert input_path.startswith("s3a://"),  f"input_path должен начинаться с s3a://, получили: {input_path}"
    assert output_path.startswith("s3a://"), f"output_path должен начинаться с s3a://, получили: {output_path}"

    print(f"[INFO] input_path:  {input_path}")
    print(f"[INFO] output_path: {output_path}")
    print(f"[INFO] report_date: {report_date}")

    if not input_path or not output_path:
        print("[ERROR] Нужно передать input_path и output_path!")
        sys.exit(1)

    # Создаём SparkSession
    spark = (
        SparkSession.builder
        .appName("LoanApplicationsProcessing")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[INFO] SparkSession создан успешно")

    # Читаем CSV
    print(f"[INFO] Читаем данные из {input_path}...")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    count = df.count()
    print(f"[INFO] Загружено записей: {count:,}")
    df.printSchema()

    # Базовые трансформации
    df_clean = (
        df
        .filter(F.col("application_id").isNotNull())
        .withColumn("report_date",   F.lit(report_date))
        .withColumn("is_approved",
            F.when(F.lower(F.col("decision_status")) == "approved", True)
             .otherwise(False)
        )
        .withColumn("amount_bucket",
            F.when(F.col("requested_amount") < 1000,   "micro")
             .when(F.col("requested_amount") < 10000,  "small")
             .when(F.col("requested_amount") < 50000,  "medium")
             .when(F.col("requested_amount") < 200000, "large")
             .otherwise("xlarge")
        )
    )

    print(f"[INFO] После очистки: {df_clean.count():,} записей")

    # Агрегат по регионам
    df_agg = (
        df_clean
        .groupBy("region_code", "product_type", "decision_status")
        .agg(
            F.count("*")                              .alias("total"),
            F.sum(F.col("is_approved").cast("int"))   .alias("approved"),
            F.avg("requested_amount")                 .alias("avg_requested"),
            F.avg("credit_score")                     .alias("avg_score"),
        )
        .withColumn("approval_rate",
            F.round(F.col("approved") / F.col("total") * 100, 2)
        )
    )

    # Сохраняем результаты
    clean_path = f"{output_path}/clean"
    agg_path   = f"{output_path}/aggregated"

    print(f"[INFO] Сохраняем чистые данные в {clean_path}...")
    (
        df_clean
        .repartition(2)
        .write
        .mode("overwrite")
        .parquet(clean_path)
    )

    print(f"[INFO] Сохраняем агрегат в {agg_path}...")
    (
        df_agg
        .repartition(1)
        .write
        .mode("overwrite")
        .parquet(agg_path)
    )

    # Итоговая статистика
    print("\n" + "="*50)
    print("ИТОГОВАЯ СТАТИСТИКА")
    print("="*50)
    stats = df_clean.agg(
        F.count("*")                             .alias("total"),
        F.sum(F.col("is_approved").cast("int"))  .alias("approved"),
        F.avg("credit_score")                    .alias("avg_score"),
    ).collect()[0]

    print(f"  Всего записей:  {stats['total']:>10,}")
    print(f"  Одобрено:       {stats['approved']:>10,}")
    print(f"  Средний скор:   {stats['avg_score']:>10.1f}")
    print("="*50)

    spark.stop()
    print("[INFO] Задание завершено успешно!")


if __name__ == "__main__":
    main()
