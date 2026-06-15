import ydb
import ydb.iam
import random
import uuid
from datetime import datetime, timedelta

YDB_ENDPOINT = "grpcs://lb.etn7augp3lb16bvnp2ru.ydb.mdb.yandexcloud.net:2135"
YDB_DATABASE = "/ru-central1/b1g9d71c6b077a05vhpb/etn7augp3lb16bvnp2ru"
IAM_TOKEN = "***"                   

REGIONS = ["DE-HE", "DE-BY", "DE-BE", "FR-IDF", "FR-PAC", "PL-MA",
           "US-CA", "US-NY", "US-TX", "GB-ENG", "IT-LOM", "ES-CAT"]

CAMPAIGN_TYPES = ["credit_card_offer", "loan_offer", "insurance",
                  "deposit_offer", "mortgage", "investment"]

CALL_STATUSES = ["answered", "no_answer", "busy", "voicemail", "callback"]

CLIENT_RESPONSES = ["interested", "not_interested", "callback_requested",
                    "already_has_product", "think_about_it", "declined"]


def random_datetime(start_year=2026):
    """Генерирует случайную дату-время в 2026 году"""
    start = datetime(start_year, 1, 1)
    end = datetime(start_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    dt = start + timedelta(days=random_days, seconds=random_seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def generate_batch(batch_num, batch_size=1000):
    """Генерирует батч записей"""
    rows = []
    for i in range(batch_size):
        idx = batch_num * batch_size + i
        call_id = f"call_{uuid.uuid4().hex[:16]}_{idx:08d}"
        row = {
            "call_id": call_id,
            "call_time": random_datetime(),
            "client_id": f"client_{random.randint(1000, 99999)}",
            "region_code": random.choice(REGIONS),
            "campaign_type": random.choice(CAMPAIGN_TYPES),
            "call_status": random.choice(CALL_STATUSES),
            "client_response": random.choice(CLIENT_RESPONSES),
            "duration_sec": random.randint(10, 900),
            "follow_up_required": random.choice(["true", "false"]),
        }
        rows.append(row)
    return rows


def upsert_batch(session, rows):
    """Загружает батч в YDB через UPSERT"""
    values_parts = []
    for r in rows:
        values_parts.append(
            f'("{r["call_id"]}", '
            f'"{r["call_time"]}", '
            f'"{r["client_id"]}", '
            f'"{r["region_code"]}", '
            f'"{r["campaign_type"]}", '
            f'"{r["call_status"]}", '
            f'"{r["client_response"]}", '
            f'{r["duration_sec"]}, '
            f'"{r["follow_up_required"]}")'
        )
    values_str = ",\n    ".join(values_parts)

    query = f"""
UPSERT INTO `transactions_v2`
    (call_id, call_time, client_id, region_code, campaign_type,
     call_status, client_response, duration_sec, follow_up_required)
VALUES
    {values_str};
"""
    session.transaction().execute(query, commit_tx=True)


def main():
    driver_config = ydb.DriverConfig(
        YDB_ENDPOINT,
        YDB_DATABASE,
        credentials=ydb.credentials.AccessTokenCredentials(IAM_TOKEN),
    )

    with ydb.Driver(driver_config) as driver:
        driver.wait(fail_fast=True, timeout=10)
        with ydb.SessionPool(driver) as pool:
            TOTAL_BATCHES = 400
            BATCH_SIZE = 500

            print(f"Начинаем загрузку {TOTAL_BATCHES * BATCH_SIZE:,} записей...")

            for batch_num in range(TOTAL_BATCHES):
                rows = generate_batch(batch_num, BATCH_SIZE)
                pool.retry_operation_sync(lambda s, r=rows: upsert_batch(s, r))

                if (batch_num + 1) % 10 == 0:
                    print(f"  Загружено батчей: {batch_num + 1}/{TOTAL_BATCHES} "
                          f"({(batch_num + 1) * BATCH_SIZE:,} записей)")

            print("Загрузка завершена")
            print(f"Итого записей: {TOTAL_BATCHES * BATCH_SIZE:,}")


if __name__ == "__main__":
    main()
