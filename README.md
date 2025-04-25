# DuckDB Quant Pipeline

A modern data pipeline for extracting Google Sheets data, transforming with DuckDB & dbt, and orchestrating with Airflow.

## 🛠️ Tech Stack

### **Data Pipeline Core**

| Component       | Description                          | Badge |
|-----------------|--------------------------------------|-------|
| **Extract**     | Google Sheets API (v4)               | [![Google Sheets](https://img.shields.io/badge/Google%20Sheets-34A853?style=flat-square&logo=google-sheets&logoColor=white)](https://developers.google.com/sheets/api)
| **Load**        | DuckDB embedded OLAP                 | [![DuckDB](https://img.shields.io/badge/DuckDB-FFF056?style=flat-square&logo=duckdb&logoColor=black)](https://duckdb.org) |
| **Transform**   | dbt with DuckDB adapter              | [![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://docs.getdbt.com) |

### **Orchestration & Visualization**

| Component       | Description                          | Badge |
|-----------------|--------------------------------------|-------|
| **Workflow**    | Airflow DAGs with Cosmos             | [![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org) |
| **Dashboards**  | Superset analytics                   | [![Superset](https://img.shields.io/badge/Superset-2598F9?style=flat-square&logo=apachesuperset&logoColor=white)](https://superset.apache.org) |


## 🚀 Getting Started

### 💻 Prerequisites
- Python 3.10-3.11
- [UV](https://docs.astral.sh/uv/) (recommended) or pip 23+
- Google Cloud Service Account credentials

### 📚 Dependency Management

This project uses **`pyproject.toml`** as the single source of truth for dependencies, structured into logical groups


### 📦 Installation
```bash
# Create and activate virtual environment
uv venv
source .venv/bin/activate  # Linux/Mac

# Install dependencies (choose one)
uv pip install -e ".[extract]"              # Minimal (extraction only)
uv pip install -e ".[extract,transform]"    # Extraction + transformation
uv pip install -e ".[all]"                  # Full setup (including Airflow/Superset)
uv pip sync pyproject.toml                  # Sync environment (clean up unused packages)
```
