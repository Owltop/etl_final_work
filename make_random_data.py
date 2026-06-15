import random
import uuid
from datetime import datetime, timedelta

OUTPUT_FILE = "insert_transactions.yql"

REGIONS = ["DE-HE", "DE-BY", "DE-BE", "FR-IDF", "FR-PAC", "PL-MA",
           "US-CA", "US-NY", "US-TX", "GB-ENG", "IT-LOM", "ES-CAT"]

CAMPAIGN_TYPES = ["credit_card_offer", "loan_offer", "insurance",
                  "deposit_offer", "mortgage", "investment"]

CALL_STATUSES = ["answered", "no_answer", "busy", "voicemail", "callback"]

CLIENT_RESPONSES = ["interested", "not_interested", "callback_requested",
                    "already_has_product", "think_about_it", "declined"]


def random_datetime():
    start = datetime(2026, 1, 1)
    delta = timedelta(days=random.randint(0, 364), seconds=random.randint(0, 86400))
    return (start + delta).strftime("%Y-%m-%d %H:%M:%S")


def generate_row(idx):
    return {
        "call_id": f"call_{uuid.uuid4().hex[:16]}_{idx:08d}",
        "call_time": random_datetime(),
        "client_id": f"client_{random.randint(1000, 99999)}",
        "region_code": random.choice(REGIONS),
        "campaign_type": random.choice(CAMPAIGN_TYPES),
        "call_status": random.choice(CALL_STATUSES),
        "client_response": random.choice(CLIENT_RESPONSES),
        "duration_sec": random.randint(10, 900),
        "follow_up_required": random.choice(["true", "false"]),
    }


TOTAL_ROWS = 200000
BATCH_SIZE = 500  # YQL UPSERT за один раз

print(f"Генерируем {TOTAL_ROWS:,} строк в файл {OUTPUT_FILE}...")

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for batch_start in range(0, TOTAL_ROWS, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, TOTAL_ROWS)
        rows = [generate_row(i) for i in range(batch_start, batch_end)]

        values = ",\n    ".join(
            f'("{r["call_id"]}", "{r["call_time"]}", "{r["client_id"]}", '
            f'"{r["region_code"]}", "{r["campaign_type"]}", "{r["call_status"]}", '
            f'"{r["client_response"]}", {r["duration_sec"]}, "{r["follow_up_required"]}")'
            for r in rows
        )

        f.write(f"UPSERT INTO `transactions_v2`\n")
        f.write(f"    (call_id, call_time, client_id, region_code, campaign_type,\n")
        f.write(f"     call_status, client_response, duration_sec, follow_up_required)\nVALUES\n    ")
        f.write(values)
        f.write(";\n\n")

        if (batch_start // BATCH_SIZE + 1) % 50 == 0:
            print(f"  {batch_end:,} / {TOTAL_ROWS:,} строк...")

print(f"Готово! Файл: {OUTPUT_FILE}")