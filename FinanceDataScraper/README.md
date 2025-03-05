
# FinanceDataProject

A distributed system for scraping financial data using Airflow, Cassandra, and Docker Swarm.

## Project Structure
- `docker/`: Docker configurations for Airflow, Cassandra, and Swarm.
- `airflow/`: Airflow DAGs, logs, and plugins.
- `FinanceDataScraper/`: Python package for scraping financial data.
- `scripts/`: Shell scripts for initializing and deploying Swarm.


## environment
Create env & install package using conda for this project

-   conda env create -f env.yml
-   conda activate financial-data-scraper
-   conda env update --file env.yml

## Setup
1. Initialize Docker Swarm:
   ```bash
   ./scripts/init-swarm.sh