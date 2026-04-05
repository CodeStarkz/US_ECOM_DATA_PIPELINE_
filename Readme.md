# US E-Commerce Data Pipeline

An end-to-end **Apache Airflow** data pipeline for ingesting, transforming, loading, and archiving U.S. e-commerce transaction data.

This project demonstrates a practical ETL workflow built with Docker, Airflow, Pandas,SMTP and MySQL. It uses local folders as staged storage layers to simulate a modern data pipeline structure.

And send Mail for Failure/ Sucess of task.

---

## Project Overview

The pipeline automates the movement of a CSV file through multiple processing stages:

- **Raw ingestion** from the input directory
- **Intermediate storage** for temporary handling
- **Data transformation** for type cleanup and standardization
- **Warehouse loading** into MySQL
- **Backup archiving** for processed files
- ** **Mail** Faor Failure or Success of Task

The design reflects a simple but realistic data engineering workflow that can be extended to cloud-based storage systems later.

---

## Pipeline Workflow

The DAG runs in the following sequence:

1. **Check for input file**
   - Verifies that the source CSV file is available in the raw data directory.

2. **Move file to intermediate storage**
   - Transfers the raw file into a working folder for processing.

3. **Transform the dataset**
   - Converts invoice numbers to integer format where applicable
   - Parses invoice dates into datetime values
   - Converts country values into categorical type
   - Saves the cleaned file to the staging/output storage

4. **Load data into the warehouse**
   - Inserts the transformed dataset into a MySQL table

5. **Move processed file to backup storage**
   - Archives the final transformed file for retention
6. ** Sent Email**
   - For failure or Sucess of Task

---

## Storage Structure

The project uses dedicated folders to represent each stage of the pipeline:

- `data/` — raw input files
- `Intermediat_Storage/` — temporary working location
- `S3_Blob_Storage/` — transformed output storage
- `Back_Up_Cold_Storage/` — archive/backup location

These names are intentionally preserved from the project structure to keep the pipeline aligned with the current implementation.

---

## Tech Stack
- **Apache Airflow** — workflow orchestration
- **Python** — pipeline logic
- **Pandas** — data loading and transformation
- **MySQL** — warehouse database
- **PostgreSQL** — Airflow metadata database
- **Redis** — Celery message broker
- **Docker Compose** — local containerized setup
- **SMTP** - For mailing

## Dataset Notes

The pipeline works with a retail e-commerce CSV dataset containing fields such as:

- Invoice number
- Stock code
- Product description
- Quantity
- Invoice date
- Unit price
- Customer ID
- Country
The transformation step helps standardize column types and improve downstream storage consistency.

---

## Project Structure




---

## How It Works

The project is containerized and designed to run locally with Airflow services, supporting a reliable development and testing environment.

### Main Services
- Airflow Webserver
- Airflow Scheduler
- Airflow Worker
- Airflow Initialization
- PostgreSQL
- MySQL
- Redis

### Airflow UI
Once the stack is running, open:

- `http://localhost:8080`

---

## Setup Instructions

### 1. Configure environment variables
Make sure the `.env` file contains the required Airflow and database settings.

### 2. Start the containers
Use Docker Compose to start the full stack.

### 3. Initialize Airflow
Run the initialization service if needed to set up the metadata database and admin user.

### 4. Trigger the DAG
Enable and run the `us_ecom_pipeline` DAG from the Airflow UI.

---

## Database Output

Processed data is loaded into the MySQL warehouse:

- **Database:** `usdb`
- **Table:** `us_ecom_data`

---

## Key Features

- Automated file-based ETL workflow
- Layered storage design
- Data type transformation and cleanup
- Warehouse loading into MySQL
- Backup of processed data
- Containerized local execution
---

## Summary

This project is a compact but practical demonstration of an Airflow-based data engineering pipeline. It shows how raw transactional data can be moved through structured storage layers, transformed, loaded into a warehouse, and archived for future reference.

---

## Future Works 
1. Add more features to the pipeline.
2. Add an alerting system. (DONE)
3. Add a cloud storage integration.
4. Add incremental loading support.
5. Add schema validation before loading.

Built as a hands-on data engineering project for learning and demonstrating ETL orchestration with Apache Airflow.
