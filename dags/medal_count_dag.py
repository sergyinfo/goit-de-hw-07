from __future__ import annotations

import pendulum
import random
import time

from airflow.models.dag import DAG

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor

# --- Конфігурація ---
MYSQL_CONN_ID = "mysql_olympic_data"
TARGET_SCHEMA = "neo_data"
TARGET_TABLE = "medal_counts"
SOURCE_SCHEMA = "neo_data"
SOURCE_TABLE = "athlete_event_results"

FULL_TARGET_TABLE = f"{TARGET_SCHEMA}.{TARGET_TABLE}"
FULL_SOURCE_TABLE = f"{SOURCE_SCHEMA}.{SOURCE_TABLE}"
DELAY_SECONDS = 35  # Час затримки для тестування сенсора
SENSOR_INTERVAL_SECONDS = 30 # Інтервал перевірки сенсора

# --- Python Функції ---

def choose_random_medal_type() -> str:
    """Випадково обирає тип медалі."""
    medal_types = ["Bronze", "Silver", "Gold"]
    selected_medal = random.choice(medal_types)
    print(f"Обраний тип медалі: {selected_medal}")
    return selected_medal

def simulate_processing_delay(seconds: int):
    """Імітує затримку обробки."""
    print(f"Починаємо імітацію затримки на {seconds} секунд.")
    time.sleep(seconds)
    print(f"Завершено імітацію затримки.")

def branch_on_medal_type(**kwargs):
    """Визначає наступне завдання на основі обраної медалі."""
    ti = kwargs["ti"]
    medal_type = ti.xcom_pull(task_ids="select_medal_type_task")
    return f"count_{medal_type.lower()}_medals_task"

# --- Аргументи DAG ---
default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2025, 5, 28, tz="UTC"),
    "retries": 0,
    "catchup": False,
}

# --- Визначення DAG ---
with DAG(
    dag_id="goit_hw7_olympic_medals",
    default_args=default_args,
    schedule=None,
    tags=["goit", "hw7", "mysql", "sensor", "sqlcommon"],
    doc_md="""
    ### DAG для Домашнього Завдання №7 (v3)

    1. Створює таблицю `medal_counts`.
    2. Обирає випадкову медаль (Gold, Silver, Bronze).
    3. Рахує кількість медалей обраного типу з `neo_data.athlete_event_results`.
    4. Записує результат у `medal_counts`.
    5. Імітує затримку (35с).
    6. Перевіряє наявність запису за останні 30с (має впасти через timeout).
    """,
) as dag:

    # --- Завдання 1: Створення таблиці ---
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_medal_counts_table_task",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {FULL_TARGET_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10) NOT NULL,
            count INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_created_at (created_at),
            INDEX idx_medal_type (medal_type)
        );
        """,
    )

    # --- Завдання 2: Вибір медалі ---
    select_medal_type_task = PythonOperator(
        task_id="select_medal_type_task",
        python_callable=choose_random_medal_type,
    )

    # --- Завдання 3: Розгалуження ---
    branch_on_medal_task = BranchPythonOperator(
        task_id="branch_on_medal_task",
        python_callable=branch_on_medal_type,
    )

    # --- Завдання 4: Підрахунок медалей (для кожного типу) ---
    medal_types = ["Gold", "Silver", "Bronze"]
    count_tasks = []

    for medal in medal_types:
        task = SQLExecuteQueryOperator(
            task_id=f"count_{medal.lower()}_medals_task",
            conn_id=MYSQL_CONN_ID,
            sql=f"""
            INSERT INTO {FULL_TARGET_TABLE} (medal_type, count)
            SELECT '{medal}', COUNT(*)
            FROM {FULL_SOURCE_TABLE}
            WHERE medal = '{medal}';
            """,
        )
        count_tasks.append(task)

    # --- Завдання 5: Затримка ---
    delay_task = PythonOperator(
        task_id="simulate_delay_task",
        python_callable=simulate_processing_delay,
        op_kwargs={"seconds": DELAY_SECONDS},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # --- Завдання 6: Сенсор перевірки ---
    verify_record_task = SqlSensor(
        task_id="verify_recent_record_task",
        conn_id=MYSQL_CONN_ID,
        sql=f"""
        SELECT COUNT(*) > 0
        FROM {FULL_TARGET_TABLE}
        WHERE created_at >= NOW() - INTERVAL {SENSOR_INTERVAL_SECONDS} SECOND;
        """,
        mode="poke",
        poke_interval=10,
        timeout=60,
    )

    # --- Визначення залежностей між завданнями ---
    create_table_task >> select_medal_type_task >> branch_on_medal_task
    branch_on_medal_task >> count_tasks
    count_tasks >> delay_task
    delay_task >> verify_record_task