FROM apache/airflow:3.0.1

# 1. Перемикаємося на root
USER airflow

# 2. Копіюємо requirements
COPY requirements.txt /requirements.txt

# 3. Встановлюємо пакети (як root, це має призвести до глобальної установки)
RUN pip install --no-cache-dir -r /requirements.txt

# 4. (Опційно, але корисно для діагностики) Перевіряємо, де встановилося
RUN pip show apache-airflow-providers-mysql

# 5. Повертаємося до користувача airflow (важливо для безпеки та правильної роботи Airflow)
USER airflow