# Итоговая работа. Модуль 4 (экзамен) — ETL-процессы

Реализация ETL/Streaming-процессов на стеке Yandex Cloud: перенос данных через
Data Transfer, автоматизация Yandex Data Processing через Apache Airflow,
потоковая аналитика поверх Apache Kafka и визуализация в DataLens.

## Состав работы

| № | Задание | Папка | Стек |
|---|---------|-------|------|
| 1 | Перенос данных YDB → Object Storage через Data Transfer | [first_task/](first_task/) | Managed YDB, Data Transfer, Object Storage, YQL |
| 2 | Автоматизация Yandex Data Processing через Apache Airflow | [second_task/](second_task/) | Managed Airflow, Data Processing, PySpark, S3 |
| 3 | Чтение топиков Apache Kafka через PySpark | [third_task/](third_task/) | Managed Kafka, Data Processing, PySpark Structured Streaming |
| 4 | Визуализация в DataLens | [fourth_task/](fourth_task/) | DataLens |

У каждого задания — свой подробный `README.md` со скриншотами и описанием шагов.

## Общая инфраструктура

| Компонент | Значение |
|-----------|----------|
| Object Storage (бакет) | `dataproc-bucket-789` |
| Managed Service for YDB | `hw-ydb-db` |
| Managed Service for Kafka | `dataproc-kafka`, топик `dataproc-kafka-topic` |
| Yandex Data Processing | `dataproc-cluster` (задание 3), временный кластер из DAG (задание 2) |
| Managed Airflow | `airflow-cluster` |
| Зона | `ru-central1-b` |

Доступ к Object Storage из Data Processing — под сервисным аккаунтом кластера
(`dataproc-sa` с ролью `storage.editor`), без статических ключей.

---

## Задание 1. Yandex DataTransfer (YDB → Object Storage)

Перенос таблицы `transactions_v2` (звонки клиентам в маркетинговых кампаниях)
из Managed YDB в Object Storage через сервис Data Transfer.

- Создана serverless-база YDB, таблица `transactions_v2` (схема 1-в-1 из задания).
- Загружено 200 000 записей Python-скриптом через YDB SDK.
- Настроены эндпоинты YDB-источник → S3-приёмник, создан и активирован
  трансфер `ydb-to-s3-transfer`, данные выгружены в бакет.

SQL-скрипты (YQL), генераторы данных и скриншоты — в [first_task/](first_task/).

## Задание 2. Автоматизация Data Processing через Airflow

DAG `loan_applications_processing` полностью автоматизирует обработку заявок
на кредит: **создать кластер → запустить PySpark → удалить кластер**.

- PySpark-задание читает CSV из `loan-applications/input/` (425 000 строк,
  ~50.7 МБ, разный объём по дням), считает производные поля и агрегаты,
  пишет результат в Parquet (`output/clean`, `output/aggregated`).
- Кластер создаётся под уникальным именем (`uuid4`), на дешёвых пресетах
  `s2.small` + HDD, удаляется по `trigger_rule=ALL_DONE`.
- Раздел «Дебаг» в README задания фиксирует все грабли (квоты, роли SA,
  S3-доступ без ключей, `fs.defaultFS` и HDFS).

Код DAG, PySpark-задание, генератор данных и скриншоты — в [second_task/](second_task/).

## Задание 3. Apache Kafka + PySpark

Потоковая и пакетная обработка топика Kafka с разворачиванием **вложенного**
JSON в плоскую таблицу.

- `kafka-producer.py` — генерирует 100 000 вложенных JSON-сообщений
  (`customer{…}`, `loan{…}`, `scoring{…}`, `documents[]`) и пишет в топик
  (~26.7 МБ).
- `kafka-read-flat.py` — пакетное чтение, парсинг вложенного JSON по
  `StructType`, flatten через точечную нотацию + `explode_outer` массива
  `documents`, запись в Parquet и CSV.
- `kafka-read-stream-flat.py` — то же потоковым `readStream` + `trigger(once=True)`.

Скрипты и скриншоты заданий — в [third_task/](third_task/).

## Задание 4. Визуализация в DataLens

Дашборд **«Аналитика кредитных заявок — Kafka»** на CSV-выгрузке из задания 3:

- распределение по уровню риска (круговая);
- статусы решений по заявкам (столбчатая);
- средняя сумма кредита по регионам (линейчатая);
- распределение скорингового балла (гистограмма).

Подробности и скриншот — в [fourth_task/](fourth_task/).

---

## Структура репозитория

```
etl_final_work/
├── first_task/     # YDB → Object Storage (Data Transfer)
├── second_task/    # Airflow + Data Processing (PySpark)
├── third_task/     # Kafka + PySpark (flatten вложенного JSON)
├── fourth_task/    # DataLens
└── README.md       # этот файл
```
