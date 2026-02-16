# INTRODUCTION
This is a real-time telecom streaming analytics platform that processes high-volume network telemetry data using an event-driven architecture.

The system ingests telecom events via Apache Kafka, processes them with PySpark Structured Streaming, stores real-time aggregates in Redis, persists historical data in PostgreSQL, and visualizes KPIs through an interactive dashboard.

This project demonstrates production-style streaming design patterns including hot/cold data paths, windowed aggregations, and distributed processing.

# Architecture Overview
<img width="1091" height="661" alt="Untitled Diagram drawio (1)" src="https://github.com/user-attachments/assets/d8a30442-b229-4a3f-93d7-37ca401671c6" />
Event Generation Layer

1. Python-based telecom event simulator
  Generates call records, tower metrics, and signal data

2. Ingestion Layer
  Apache Kafka cluster
  Distributed, fault-tolerant event streaming

3. Processing Layer
  PySpark Structured Streaming
  Windowed aggregations
  KPI computation
  Real-time metric transformation

4. Storage Layer
  Redis (Hot Path – Low latency metrics)
  PostgreSQL (Cold Path – Historical persistence)

5. Visualization Layer
  Interactive dashboard (Grafana-style)
  Live network KPIs and historical trends

# To run this
1. Clone the repository to your local directory
2. Run 'docker-compose up'


