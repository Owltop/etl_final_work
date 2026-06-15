# Задание 2. Автоматизация Yandex Data Processing через Apache Airflow

## Описание

ETL-пайплайн для обработки файлов заявок на кредит (CSV → Parquet)
с автоматическим созданием и удалением кластера Yandex Data Processing
через Apache Airflow.

**Логика DAG:**
1. Создать кластер Yandex Data Processing
2. Запустить PySpark-задание (CSV → Parquet + агрегаты)
3. Удалить кластер

---

## Инфраструктура

| Компонент | Значение |
|-----------|----------|
| Managed Airflow | `airflow-cluster` |
| S3 бакет | `dataproc-bucket-789` |
| Сервисный аккаунт кластера | `dataproc-sa` |
| Зона | `ru-central1-b` |
| Образ кластера | `2.1` (Spark 3.x) |

### Роли сервисного аккаунта `dataproc-sa`

| Роль | Зачем |
|------|-------|
| `dataproc.agent` | Работа кластера Data Processing |
| `storage.editor` | Чтение и запись в S3 |
| `storage.uploader` | Загрузка файлов в S3 |
| `storage.viewer` | Просмотр файлов в S3 |
| `compute.editor` | Создание VM для нод кластера |
| `iam.serviceAccounts.user` | Использование сервисного аккаунта |

---

## Структура данных

### Входные данные

```
s3://dataproc-bucket-789/loan-applications/input/
├── loan_applications_20260501.csv
├── loan_applications_20260502.csv
├── loan_applications_20260503.csv
├── loan_applications_20260504.csv
└── loan_applications_20260505.csv
```

Итого: **350 000 строк**

### Схема входного CSV

| Поле | Тип | Описание |
|------|-----|----------|
| application_id | string | Уникальный ID заявки |
| event_time | string | Дата и время заявки |
| customer_id | string | ID клиента |
| region_code | string | Код региона |
| product_type | string | Тип продукта |
| requested_amount | double | Запрошенная сумма |
| term_months | int | Срок в месяцах |
| credit_score | int | Кредитный скор |
| risk_level | string | Уровень риска |
| decision_status | string | Решение по заявке |
| approved_amount | double | Одобренная сумма |
| channel | string | Канал обращения |
| employee_review_flag | string | Флаг ручной проверки |
| processing_time_sec | int | Время обработки (сек) |

### Выходные данные (Parquet)

```
s3://dataproc-bucket-789/loan-applications/output/
├── clean/          — очищенные данные с новыми полями
└── aggregated/     — агрегат по регионам и продуктам
```

---

## Файлы

| Файл | Описание |
|------|----------|
| `scripts/generate_data.py` | Генератор тестовых CSV-данных |
| `scripts/process_loan_applications.py` | PySpark-задание |
| `dags/dag_loan_processing.py` | DAG для Airflow |

---

## Конфигурация кластера Data Processing

```
Мастер-нода:    s2.small, HDD 20 ГБ
Compute-нода:   s2.small, HDD 20 ГБ × 1
Data-нода:      s2.small, HDD 20 ГБ × 1
Сервисы:        YARN, SPARK, HDFS
Образ:          2.1
```

---

## Запуск

DAG запускается автоматически по расписанию `@daily`.

Для ручного запуска:
1. Airflow UI → DAG `loan_applications_processing`
2. Кнопка **Trigger DAG**

---

## Дебаг: что пошло не так и как починили

Это задание потребовало значительного количества отладки.
Фиксируем все грабли чтобы другие не наступали.

### Проблема 1 — `Cluster.Name: pattern mismatch`

**Симптом:**
```
INVALID_ARGUMENT: Cluster.Name: pattern mismatch
```

**Причина:**  
Имя кластера формировалось через Jinja-шаблон:
```python
cluster_name="loan-processing-{{ ds_nodash }}"
```
Jinja-шаблоны не работают в параметре `cluster_name` оператора —
строка передаётся как есть со спецсимволами `{` `}`.

**Решение:**  
Использовать `uuid.uuid4()` для генерации уникального имени:
```python
cluster_name=f"tmp-dp-{uuid.uuid4()}"
```

---

### Проблема 2 — неверные названия параметров нод

**Симптом:**
```
Invalid arguments were passed to DataprocCreateClusterOperator.
Invalid arguments were: **kwargs: {'deletion_protection': False}
```
и далее ошибки про `datanode_resource_preset`.

**Причина:**  
Использовали параметры старой версии оператора:
```python
datanode_resource_preset="s3-c4-m16"  # не существует
deletion_protection=False              # не поддерживается
```

**Решение:**  
Использовать актуальные параметры как в документации:
```python
computenode_resource_preset="s2.small"
# deletion_protection убрать совсем
```

---

### Проблема 3 — превышение квоты Compute

**Симптом:**
```
insufficient Compute quota for:
compute.ssdDisks.size required 644245094400 but available 42949672960
compute.instanceMemory.size required 154618822656 but available 137438953472
```

**Причина:**  
Изначально задали слишком большие машины (`m2.large`) и диски (200 ГБ SSD).
Квота облака: **40 ГБ SSD** и **128 ГБ RAM**.

**Решение:**  
- Уменьшить пресет до `s2.small`
- Уменьшить диски до 20 ГБ
- Заменить `network-ssd` на `network-hdd` (HDD не ест SSD квоту)
- Предварительно удалить старые кластеры Data Processing которые
  занимали квоту

---

### Проблема 4 — не хватает ролей сервисному аккаунту

**Симптом (серия ошибок):**
```
service account must have role compute.instanceGroups.create
service account must have role iam.serviceAccounts.use
```

**Причина:**  
Сервисному аккаунту `dataproc-sa` не хватало ролей для
создания кластера.

**Решение:**  
Добавить роли через CLI:
```bash
yc resource-manager folder add-access-binding \
  --id <folder_id> \
  --role compute.editor \
  --service-account-id <dataproc-sa-id>

yc resource-manager folder add-access-binding \
  --id <folder_id> \
  --role iam.serviceAccounts.user \
  --service-account-id <dataproc-sa-id>
```

---

### Проблема 5 — `NoAwsCredentialsException` при чтении скрипта

**Симптом:**
```
NoAwsCredentialsException: SimpleAWSCredentialsProvider:
No AWS credentials in the Hadoop configuration
```

**Причина:**  
Spark не мог скачать сам PySpark-скрипт из S3 —
credentials не были переданы кластеру.

**Первая попытка решения (неверная):**  
Передать ключи через `spark.hadoop.fs.s3a.*` в properties задания.
Это помогло скачать скрипт, но не помогло executor-ам.

**Итоговое решение (неверное — см. Проблему 6):**  
Передавать статические ключи через `core:fs.s3a.*` на уровне кластера.

---

### Проблема 6 — `Access Denied` при ЗАПИСИ в S3 из executor-ов

**Симптом:**
```
java.nio.file.AccessDeniedException: loan-applications/output/clean:
PUT 0-byte object: Access Denied (Status Code: 403)
```

Причём путь **без** `s3a://bucket/` — относительный!

**Причина:**  
При явной передаче статических S3-ключей через `core:fs.s3a.access.key`
возникал конфликт: executor-контейнеры YARN брали credentials из
системной конфигурации кластера (где прописан служебный бакет
Data Processing), а не из наших переданных ключей.
В итоге executor писал по относительному пути без бакета.

**Решение:**  
**Убрать статические ключи совсем.**  
Yandex Data Processing умеет обращаться к S3 под сервисным аккаунтом
кластера (`service_account_id`) без явных ключей — это штатный механизм.
Достаточно что у `dataproc-sa` есть роль `storage.editor`.

```python
# НЕПРАВИЛЬНО — явные ключи конфликтуют с системной конфигурацией
properties={
    "core:fs.s3a.access.key": "KEY...",
    "core:fs.s3a.secret.key": "SECRET...",
}

# ПРАВИЛЬНО — без ключей, аутентификация через IAM сервисного аккаунта
properties={
    "core:fs.s3a.endpoint":               "https://storage.yandexcloud.net",
    "core:fs.s3a.path.style.access":      "true",
    "core:fs.s3a.connection.ssl.enabled": "true",
    # access.key и secret.key НЕ указываем
}
```

---

### Проблема 7 — `fs.defaultFS` ломал запуск кластера

**Симптом:**  
Кластер падал при инициализации когда пробовали:
```python
"core:fs.defaultFS": "s3a://dataproc-bucket-789"
```

**Причина:**  
При наличии HDFS (`datanode_count=1`) кластер использует HDFS
как дефолтную файловую систему для служебных операций
(`/user`, `/var`, bootstrap-скрипты).
Переопределение `fs.defaultFS` на S3 ломало эти операции.

**Решение:**  
Не переопределять `fs.defaultFS`. Обращаться к S3 только
по явным `s3a://bucket/path` путям внутри PySpark-скрипта.

---

## Итоговые выводы

1. **Не используй статические S3-ключи в Yandex Data Processing** —
   кластер сам умеет работать с S3 через IAM сервисного аккаунта.

2. **`cluster_name` не поддерживает Jinja** — используй `uuid.uuid4()`.

3. **HDD вместо SSD** для учебных кластеров — квота SSD очень мала.

4. **`fs.defaultFS` не переопределять** при наличии HDFS в кластере.

5. **Минимальная конфигурация** для работы PySpark + S3:
   - `services=["YARN", "SPARK", "HDFS"]`
   - `datanode_count=1` — нужен для HDFS staging
   - `datanode_disk_type="network-hdd"` — экономия SSD квоты

---

## Скриншоты

- `screens/screen1.png` — генерация данных
- `screens/screen2.png` — DAG завершен
- `screens/screen3.png` — результаты в S3
```