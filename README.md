
## Run Airflow

Follow these steps to set up and run Airflow using Docker:

1. **Navigate to the Airflow Directory**
   ```bash
   cd airflow-docker
   docker build -t my-airflow .
   docker-compose up -d
URL: http://localhost:8080 Username: airflow Password: airflow