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

1. Install dependencies using Pipenv:
   ```sh
   pipenv install

