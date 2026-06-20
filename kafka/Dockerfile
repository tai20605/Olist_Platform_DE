# ─── Kafka Simulation Producer ───────────────────────────────────────────────
# Image Python slim để chạy script gửi events vào Kafka (KRaft, không Zookeeper)
FROM python:3.11-slim

# Tránh hỏi tương tác khi cài package hệ thống
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# 1. Cài requirements trước (cache layer riêng, rebuild nhanh hơn)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy script producer
COPY simulation_produce_event.py .

# 3. Biến môi trường mặc định – có thể override trong docker-compose
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# 4. Script chạy 1 lần rồi thoát
CMD ["python", "simulation_produce_event.py"]
