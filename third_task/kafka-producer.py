#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, when, to_json, struct, array
from pyspark.sql.types import StringType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("kafka-producer-json") \
        .getOrCreate()

    NUM_RECORDS = 100000

    df = spark.range(0, NUM_RECORDS).toDF("id")

    df = df.withColumn("region",
        when(col("id") % 8 == 0, "DE-HE")
        .when(col("id") % 8 == 1, "DE-BY")
        .when(col("id") % 8 == 2, "DE-BE")
        .when(col("id") % 8 == 3, "US-CA")
        .when(col("id") % 8 == 4, "US-NY")
        .when(col("id") % 8 == 5, "RU-MOW")
        .when(col("id") % 8 == 6, "RU-SPE")
        .otherwise("FR-IDF")
    )

    df = df.withColumn("risk_level",
        when(col("id") % 3 == 0, "low")
        .when(col("id") % 3 == 1, "medium")
        .otherwise("high")
    )

    df = df.withColumn("decision_status",
        when(col("id") % 4 == 0, "approved")
        .when(col("id") % 4 == 1, "rejected")
        .when(col("id") % 4 == 2, "manual_review")
        .otherwise("pending")
    )

    df = df.withColumn("doc_status",
        when(col("id") % 3 == 0, "verified")
        .when(col("id") % 3 == 1, "pending")
        .otherwise("rejected")
    )

    df = df \
        .withColumn("application_id",
                    concat(lit("loan_"), col("id").cast(StringType()))) \
        .withColumn("customer_id",
                    concat(lit("cust_"), (col("id") % 9999).cast(StringType()))) \
        .withColumn("loan_amount",
                    ((col("id") % 49000) + 1000).cast(IntegerType())) \
        .withColumn("term_months",
                    ((col("id") % 58) + 6).cast(IntegerType())) \
        .withColumn("score",
                    ((col("id") % 650) + 350).cast(IntegerType())) \
        .withColumn("doc_type",     lit("passport")) \
        .withColumn("submitted_at", lit("2026-05-01T10:15:11Z"))

    df_kafka = df.select(
        to_json(
            struct(
                col("application_id"),
                struct(
                    col("customer_id"),
                    col("region")
                ).alias("customer"),
                struct(
                    col("loan_amount").alias("amount"),
                    col("term_months")
                ).alias("loan"),
                struct(
                    col("score"),
                    col("risk_level")
                ).alias("scoring"),
                array(
                    struct(
                        col("doc_type").alias("type"),
                        col("doc_status").alias("status")
                    )
                ).alias("documents"),
                col("decision_status"),
                col("submitted_at")
            )
        ).alias("value")
    )

    df_kafka.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                "rc1b-28ta6m9gjgtk3ele.mdb.yandexcloud.net:9091") \
        .option("topic", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=\"user1\" "
                "password=\"password1\" "
                ";") \
        .save()

    print("SUCCESS: отправлено 100000 записей в топик")
    spark.stop()

if __name__ == "__main__":
    main()
