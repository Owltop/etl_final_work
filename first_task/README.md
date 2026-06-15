# Домашняя работа: Data Engineering (Фамилия Имя)

## Задание 1. Работа с Yandex DataTransfer (YDB → Object Storage)

### Описание
Перенос данных из Managed Service for YDB в Yandex Object Storage 
с использованием сервиса Data Transfer.

### Данные
Таблица `transactions_v2` — данные о звонках клиентам в рамках 
маркетинговых кампаний.

| Поле | Тип | Описание |
|------|-----|----------|
| call_id | Utf8 | Уникальный ID звонка |
| call_time | Utf8 | Дата и время звонка |
| client_id | Utf8 | ID клиента |
| region_code | Utf8 | Код региона |
| campaign_type | Utf8 | Тип кампании |
| call_status | Utf8 | Статус звонка |
| client_response | Utf8 | Ответ клиента |
| duration_sec | Int32 | Длительность в секундах |
| follow_up_required | Utf8 | Требуется ли повторный контакт |

**Объём данных**: 200,000 записей (~35 МБ)

### Шаги выполнения

1. Создана YDB Serverless база `hw-ydb-db`
2. Создана таблица `transactions_v2`
3. Загружено 200,000 записей Python-скриптом
4. Создан сервисный аккаунт `data-transfer-sa`
5. Настроены эндпоинты YDB → S3
6. Создан и активирован трансфер `ydb-to-s3-transfer`
7. Данные успешно выгружены в `dataproc-bucket-789/ydb-transfer-output/`

### Результат
![Transfer Done](screenshots/transfer_done.png)
![S3 Files](screenshots/s3_files_result.png)

### SQL-скрипты
- [Создание таблицы](sql/1.yql)
- [Проверка количества](sql/2.yql)
- [Просмотр данных](sql/3.yql)
