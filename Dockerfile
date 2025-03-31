FROM python:3.10-slim

# Установка зависимостей системы
RUN apt-get update && apt-get install -y gcc python3-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала копируем только requirements.txt
COPY requirements.txt .

# Устанавливаем зависимости Python
RUN pip install --no-cache-dir -r requirements.txt

# Затем копируем весь остальной код
COPY . .

CMD ["python", "pars_failureConfirmationTime_and_additiional_data.py"]