FINANCEDATAPROJECT/
│
├── airflow-docker/               # Directory containing Airflow setup with Docker
│   ├── config/                   # Airflow configuration files
│   ├── dags/                     # Default Airflow DAGs folder (mapped to FinanceDataScraper/scripts)
│   ├── logs/                     # Airflow logs
│   ├── plugins/                  # Airflow plugins
│   ├── env/                      # Environment variables
│   ├── docker-compose.yaml       # Docker Compose configuration for Airflow
│   ├── Dockerfile                # Dockerfile for building Airflow image
│   └── requirements.txt          # Python dependencies for Airflow
│
├── FinanceDataScraper/           # Directory for financial data scraping logic
│   ├── config/                   # Configuration files for the scraper
│   ├── data_fetcher/             # Data fetching scripts
│   ├── data_processor/           # Data processing scripts
│   ├── database/                 # Database-related scripts
│   ├── scripts/                  # Airflow DAGs (tasks) for scraping and processing
│   ├── utils/                    # Utility scripts
│   ├── init.py                   # Initialization script
│   ├── env.yml                   # Environment configuration
│   ├── main.py                   # Main script for standalone execution
│   ├── README.md                 # README for FinanceDataScraper
│   └── requirements.txt          # Python dependencies for the scraper
│
├── scripts/                      # Additional scripts
├── .gitignore                    # Git ignore file
└── README.md                     # Project README (this file)

## Run Airflow

Follow these steps to set up and run Airflow using Docker:

1. **Navigate to the Airflow Directory**
   ```bash
   cd airflow-docker
   docker build -t my-airflow .
   docker-compose up -d
URL: http://localhost:8080
Username: airflow
Password: airflow