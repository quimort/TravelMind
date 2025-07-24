# TravelMind

TravelMind is a data engineering project focused on ingesting, processing, and exploiting tourism and climate data using PySpark and Apache Iceberg. The repository is organized into three main layers: `landing`, `trusted`, and `exploitation`, each with its own set of Jupyter notebooks and utility scripts.

## Architecture

   ![Alt text](images/diagrama.png)
   
## Repository Structure

## Data Pipeline Overview

### 1. Landing Layer

- **Purpose:** Ingests raw data from external APIs (e.g., AEMET for climate, SEGITTUR for hotels and tourism).
- **Key Notebooks:**
  - `aemet_l.ipynb`: Downloads and stores raw climate data.
  - `hoteles.ipynb`: Ingests hotel occupancy data.
  - `turismoProvincia.ipynb`: Ingests inter-provincial tourism data.
- **Utilities:**  
  - [`landing/utils.py`](landing/utils.py): Functions for Spark session creation, API data extraction, and Iceberg table operations.

### 2. Trusted Layer

- **Purpose:** Cleans, normalizes, and validates data from the landing layer, preparing it for analytics.
- **Key Notebooks:**
  - `clima_t.ipynb`: Cleans and imputes missing values in climate data.
  - `f_ocupacion_hotelera.ipynb`: Processes hotel occupancy data (normalization, type casting, filtering).
  - `turismoProvincia.ipynb`: Cleans and uppercases tourism data.
- **Utilities:**  
  - [`trusted/utils.py`](trusted/utils.py): Similar to landing utilities, adapted for trusted data operations.

### 3. Exploitation Layer

- **Purpose:** Prepares trusted data for reporting, dashboards, and advanced analytics.
- **Key Notebooks:**
  - `clima_ez.ipynb`: Selects and filters climate data for reporting.
  - `f_ocupacion_barcelona.ipynb`: Extracts and processes hotel occupancy data for Barcelona.
  - `turismoProvincia.ipynb`: Filters and exports tourism data for specific provinces.
- **Utilities:**  
  - [`exploitation/utils.py`](exploitation/utils.py): Utility functions for exploitation layer.

## Technologies Used

- **PySpark**: Distributed data processing.
- **Apache Iceberg**: Table format for large analytic datasets.
- **Jupyter Notebooks**: Interactive data engineering and analysis.
- **Pandas**: Auxiliary data manipulation.
- **Requests**: API data ingestion.

## How to Run

### 1. Levantar la arquitectura Dockerizada

Asegúrate de tener [Docker](https://www.docker.com/) y [Docker Compose](https://docs.docker.com/compose/) instalados.

```sh
docker-compose up -d
```

Esto levantará los siguientes servicios:
- **MinIO** (almacenamiento S3 para tablas Iceberg)
  - UI: [http://localhost:9001](http://localhost:9001) (usuario: `minio`, password: `minio123`)
- **Spark Master** (cluster de procesamiento)
  - UI: [http://localhost:8080](http://localhost:8080)
- **Airflow** (orquestación de pipelines)
  - UI: [http://localhost:8081](http://localhost:8081) (usuario: `airflow`, password: `airflow` por defecto)
- **Postgres** (backend de Airflow)

### 2. Inicializar Airflow

La primera vez, inicializa la base de datos y crea el usuario admin:

```sh
docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create \
    --username airflow \
    --password airflow \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com
```

### 3. Lanzar un pipeline de ejemplo

Hay un DAG de ejemplo en `dags/example_spark_dag.py` que lanza un job de Spark Pi al cluster Spark. Puedes activarlo desde la UI de Airflow.

### 4. Parar los servicios

```sh
docker-compose down
```

### 5. (Opcional) Instalar dependencias Python locales
Si quieres ejecutar notebooks o scripts locales:
```sh
pipenv install
```

