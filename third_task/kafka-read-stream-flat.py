#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode_outer
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType
)

def main():
    spark = SparkSession.builder \
        .appName("kafka-read-stream-flat") \
        .getOrCreate()

    # --- Схема ВЛОЖЕННОГО входящего JSON ---
    schema = StructType([
        StructField("application_id", StringType(), True),
        StructField("customer", StructType([
            StructField("customer_id", StringType(), True),
            StructField("region",      StringType(), True),
        ]), True),
        StructField("loan", StructType([
            StructField("amount",      IntegerType(), True),
            StructField("term_months", IntegerType(), True),
        ]), True),
        StructField("scoring", StructType([
            StructField("score",      IntegerType(), True),
            StructField("risk_level", StringType(),  True),
        ]), True),
        StructField("documents", ArrayType(StructType([
            StructField("type",   StringType(), True),
            StructField("status", StringType(), True),
        ])), True),
        StructField("decision_status", StringType(), True),
        StructField("submitted_at",    StringType(), True),
    ])

    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                "rc1b-28ta6m9gjgtk3ele.mdb.yandexcloud.net:9091") \
        .option("subscribe", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=\"user1\" "
                "password=\"password1\" "
                ";") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .where(col("json_str").isNotNull())

    df_exploded = df_stream \
        .withColumn("data", from_json(col("json_str"), schema)) \
        .select(
            col("data.application_id").alias("application_id"),
            col("data.customer.customer_id").alias("customer_id"),
            col("data.customer.region").alias("region"),
            col("data.loan.amount").alias("loan_amount"),
            col("data.loan.term_months").alias("term_months"),
            col("data.scoring.score").alias("score"),
            col("data.scoring.risk_level").alias("risk_level"),
            explode_outer(col("data.documents")).alias("doc"),
            col("data.decision_status").alias("decision_status"),
            col("data.submitted_at").alias("submitted_at"),
        )

    df_flat = df_exploded.select(
        "application_id",
        "customer_id",
        "region",
        "loan_amount",
        "term_months",
        "score",
        "risk_level",
        col("doc.type").alias("doc_type"),
        col("doc.status").alias("doc_status"),
        "decision_status",
        "submitted_at",
    )

    query = df_flat.writeStream \
        .trigger(once=True) \
        .queryName("flat_stream_messages") \
        .format("memory") \
        .start()

    query.awaitTermination()

    result = spark.sql("SELECT * FROM flat_stream_messages")

    result.write \
        .mode("overwrite") \
        .parquet("s3a://dataproc-bucket-789/kafka-stream-flat-output/")

    print(f"SUCCESS: потоковое чтение завершено, сохранено {result.count()} записей")
    result.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
