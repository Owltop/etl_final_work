"""
DAG: Автоматизация обработки заявок на кредит через Yandex Data Processing.

Доступ к Object Storage — под сервисным аккаунтом кластера (dataproc-sa),
БЕЗ статических ключей. Это штатный способ Yandex Data Processing: кластер
обращается к бакету под своим service_account_id, а права берутся из его
IAM-ролей (у dataproc-sa уже есть storage.editor / storage.uploader).
"""

import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_FOLDER_ID   = "b1gbopbjs88t932htk04"
YC_ZONE        = "ru-central1-b"
S3_BUCKET      = "dataproc-bucket-789"
DATAPROC_SA_ID = "aje3hbhbtodmj4qivv7d"   # dataproc-sa: storage.editor/uploader + dataproc.agent
SUBNET_ID      = "e2le561rnc0bgs7i68sk"
SSH_PUBLIC_KEY = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC0p7QudI7fFbxTS1P0rVDvNzRu7Tmt6YQXgHMnc/rzXwtfXEMzl06e10p5NStD7uKKoAtq8HUdwxMpBAr64sDrCH25vsbySPxSXdf6vz31Uht40q0J2X/Xu9PczyboRnt9PMaYRhugCv9Eth8ihBs9Qrlv7WbxIj+4RUxC4FvY0TMJujtsdueATnvh8cWYiVl71RCpchN+FKPYoDQzAtra4F+Y/913VvAqmk48t7KWshP+CgDuNkCAJ4Dh5+641vAEVVP0Ba1ICzBGPkIr857BNT5OkqVmjdzas9f2NbUFt2pe72YxGQyblfgoC+wR3nAs6L0AQ869pDhWSgkcbGkV owltop@owltop-osx"

PYSPARK_SCRIPT = f"s3a://{S3_BUCKET}/airflow/scripts/process_loan_applications.py"
INPUT_PATH     = f"s3a://{S3_BUCKET}/loan-applications/input/"
OUTPUT_PATH    = f"s3a://{S3_BUCKET}/loan-applications/output"

# Никаких S3_ACCESS_KEY / S3_SECRET_KEY в коде:
# аутентификация в Object Storage идёт под сервисным аккаунтом кластера.

# =============================================================

with DAG(
    dag_id="loan_applications_processing",
    schedule="@daily",
    tags=["data-processing", "loan", "etl", "pyspark"],
    start_date=datetime.datetime(2026, 6, 1),   # фиксированная дата, НЕ datetime.now()
    max_active_runs=1,
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="dp-cluster-create-task",
        cluster_name=f"tmp-dp-{uuid.uuid4()}",
        cluster_description="Временный кластер для обработки заявок на кредит",
        ssh_public_keys=SSH_PUBLIC_KEY,
        service_account_id=DATAPROC_SA_ID,
        subnet_id=SUBNET_ID,
        s3_bucket=S3_BUCKET,            # бакет для артефактов/логов задач — пишется под SA
        zone=YC_ZONE,
        cluster_image_version="2.1",
        masternode_resource_preset="s2.small",
        masternode_disk_type="network-hdd",
        masternode_disk_size=20,
        computenode_resource_preset="s2.small",
        computenode_disk_type="network-hdd",
        computenode_disk_size=20,
        computenode_count=1,
        datanode_resource_preset="s2.small",
        datanode_disk_type="network-hdd",
        datanode_disk_size=20,
        datanode_count=1,
        services=["YARN", "SPARK", "HDFS"],
        properties={
            # --- Object Storage без ключей ---
            # Не задаём fs.s3a.access.key / secret.key / aws.credentials.provider:
            # кластер ходит в бакет под своим сервисным аккаунтом (DATAPROC_SA_ID).
            "core:fs.s3a.endpoint":               "https://storage.yandexcloud.net",
            "core:fs.s3a.path.style.access":      "true",
            "core:fs.s3a.connection.ssl.enabled": "true",
            # fs.defaultFS НЕ переопределяем на s3a://. В кластере есть HDFS (datanode),
            # поэтому служебные mkdir /user, /var при бутстрапе идут в HDFS, а не в S3
            # (именно это переопределение и роняло запуск раньше).
            # К бакету обращаемся ТОЛЬКО по явным s3a://-путям внутри самой задачи.
        },
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id="dp-cluster-pyspark-task",
        main_python_file_uri=PYSPARK_SCRIPT,
        args=[
            INPUT_PATH,
            OUTPUT_PATH,
            # report_date. Сейчас вычисляется при парсинге DAG.
            # Идиоматичнее — "{{ ds }}" (логическая дата запуска), ЕСЛИ оператор
            # шаблонизирует args; иначе в колонку попадёт литерал "{{ ds }}".
            str(datetime.date.today()),
        ],
        properties={
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.adaptive.enabled":   "true",
            "spark.driver.memory":          "2g",
            "spark.executor.memory":        "2g",
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="dp-cluster-delete-task",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> run_pyspark  >> delete_cluster