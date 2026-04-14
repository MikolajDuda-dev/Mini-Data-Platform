# Mini-Data-Platform

#Overview

This project is a comprehensive, containerized data platform designed to simulate a production-grade MLOps lifecycle. It implements a Medallion Architecture (Bronze → Silver → Gold) to process data, ensure its quality, and automate machine learning workflows .The platform is orchestrated by Apache Airflow, utilizes Apache Spark for data processing, MinIO as a high-performance object store (S3-compatible), Great Expectations for data quality assurance, and MLflow for experiment tracking and model management.


#Features

-**Automated Orchestration:** Full pipeline management using Apache Airflow DAGs .
-**Scalable Data Processing:** Distributed data transformations between layers using an Apache Spark cluster.
-**Data Lake Storage:** S3-compatible storage using MinIO for persistent data layers.
-**Data Quality Framework:** Automated validation suites powered by Great Expectations.
-**MLOps Integration:** Complete experiment tracking, model versioning, and REST API serving with MLflow.
-**Containerized Infrastructure:** One-click deployment using Docker Compose, optimized for Windows 11 (WSL2).

#Architecture

1. **Ingestion (Bronze):** Raw JSON data is ingested and stored in MinIO.
2. **Processing (Silver):** Spark cleans, normalizes, and converts data into Parquet format.
3. **Quality Gate:** Great Expectations runs validation checks (null checks, range validation).
4. **ML Training:** A model is trained on the Silver data, with metrics and artifacts logged to MLflow.
5. **Model Registry:** The trained model is registered and ready for inference via REST API.

#Tech Stack

-**Orchestration:** Apache Airflow 2.10.2
-**Processing:** Apache Spark 3.5.1
-**Storage:** MinIO
-**Data Quality:** Great Expectations
-**ML Lifecycle:** MLflow 2.16.0
-**Database:** PostgreSQL 13/16
-**Language:** Python 3.11
