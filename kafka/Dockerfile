FROM python:3.10-slim

WORKDIR /app

# Pre-install python packages for mock-producer
RUN pip install --no-cache-dir kafka-python requests fastavro

# Run simulation script directly
CMD ["python", "simulation_produce_event.py"]
