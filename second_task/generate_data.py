#!/usr/bin/env python3
"""
Генератор данных для задания 2.
Создаёт CSV-файлы с заявками на кредит (>50 МБ суммарно).
Имитирует разный объём данных в разные дни месяца.
"""

import csv
import random
import os
import gzip
from datetime import datetime, timedelta

OUTPUT_DIR = "./loan_data"
NUM_FILES = 5
ROWS_PER_FILE = 80000

REGIONS = ["DE-HE", "DE-BY", "DE-BE", "FR-IDF", "FR-PAC",
           "PL-MA", "US-CA", "US-NY", "US-TX", "GB-ENG",
           "IT-LOM", "ES-CAT", "NL-NH", "SE-AB", "NO-03"]

PRODUCT_TYPES = ["cash_loan", "mortgage", "car_loan",
                 "credit_card", "microloan", "business_loan"]

CHANNELS = ["mobile", "web", "branch", "call_center", "partner"]

RISK_LEVELS = ["low", "medium", "high", "very_high"]

DECISION_STATUSES = ["approved", "rejected", "pending",
                     "manual_review", "cancelled"]


def get_decision_by_risk(risk_level):
    """Логика принятия решения зависит от риска"""
    weights = {
        "low":       [70, 5,  15, 8,  2],
        "medium":    [45, 20, 20, 12, 3],
        "high":      [20, 45, 15, 15, 5],
        "very_high": [5,  70, 5,  15, 5],
    }
    return random.choices(DECISION_STATUSES, weights=weights[risk_level])[0]


def get_approved_amount(decision, requested):
    """Одобренная сумма зависит от решения"""
    if decision == "approved":
        # Иногда одобряем меньше запрошенного
        return round(requested * random.uniform(0.7, 1.0), 2)
    elif decision == "manual_review":
        return round(requested * random.uniform(0.5, 0.9), 2)
    else:
        return 0.0


def get_credit_score_by_risk(risk_level):
    ranges = {
        "low":       (720, 850),
        "medium":    (600, 719),
        "high":      (480, 599),
        "very_high": (300, 479),
    }
    lo, hi = ranges[risk_level]
    return random.randint(lo, hi)


def random_datetime(base_date):
    """Случайное время в течение дня"""
    hour = random.randint(8, 21)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    dt = base_date.replace(hour=hour, minute=minute, second=second)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def generate_file(file_index, num_rows):
    """Генерирует один CSV-файл"""
    base_date = datetime(2026, 5, 1) + timedelta(days=file_index)
    date_str = base_date.strftime("%Y%m%d")
    filename = f"loan_applications_{date_str}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    fieldnames = [
        "application_id", "event_time", "customer_id", "region_code",
        "product_type", "requested_amount", "term_months", "credit_score",
        "risk_level", "decision_status", "approved_amount", "channel",
        "employee_review_flag", "processing_time_sec"
    ]

    print(f"  Генерируем файл: {filename} ({num_rows:,} строк)...")

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(num_rows):
            app_num = file_index * num_rows + i + 1
            risk = random.choices(
                RISK_LEVELS,
                weights=[35, 35, 20, 10]
            )[0]
            decision = get_decision_by_risk(risk)
            requested = round(random.choice([
                random.randint(500, 5000),
                random.randint(5001, 50000),
                random.randint(50001, 500000),
            ]) * 1.0, 2)
            credit_score = get_credit_score_by_risk(risk)
            approved = get_approved_amount(decision, requested)
            product = random.choice(PRODUCT_TYPES)

            # Ипотека — большие суммы и длинные сроки
            if product == "mortgage":
                requested = round(random.uniform(50000, 800000), 2)
                term = random.choice([120, 180, 240, 300, 360])
                approved = get_approved_amount(decision, requested)
            elif product == "microloan":
                requested = round(random.uniform(100, 3000), 2)
                term = random.choice([3, 6, 12])
                approved = get_approved_amount(decision, requested)
            else:
                term = random.choice([6, 12, 18, 24, 36, 48, 60])

            row = {
                "application_id":      f"app_{date_str}_{app_num:06d}",
                "event_time":          random_datetime(base_date),
                "customer_id":         f"cust_{random.randint(10000, 999999):06d}",
                "region_code":         random.choice(REGIONS),
                "product_type":        product,
                "requested_amount":    requested,
                "term_months":         term,
                "credit_score":        credit_score,
                "risk_level":          risk,
                "decision_status":     decision,
                "approved_amount":     approved,
                "channel":             random.choice(CHANNELS),
                "employee_review_flag": random.choice(
                    ["true"] * 20 + ["false"] * 80
                ),
                "processing_time_sec": random.randint(5, 3600),
            }
            writer.writerow(row)

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  ✅ {filename}: {size_mb:.1f} МБ")
    return filepath, size_mb


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("=" * 55)
    print("Генерация тестовых данных: заявки на кредит")
    print("=" * 55)

    total_mb = 0
    # Разный объём данных в разные дни (имитация реальности)
    rows_per_day = [
        100000,   # день 1 — много заявок
        55000,   # день 2 — меньше
        115000,   # день 3 — пиковый день
        75000,   # день 4
        80000,   # день 5
    ]

    for i, num_rows in enumerate(rows_per_day):
        filepath, size_mb = generate_file(i, num_rows)
        total_mb += size_mb

    print("=" * 55)
    print(f"Итого файлов: {NUM_FILES}")
    print(f"Итого размер: {total_mb:.1f} МБ")
    print(f"Файлы сохранены в: {OUTPUT_DIR}/")
    print("=" * 55)


if __name__ == "__main__":
    main()
