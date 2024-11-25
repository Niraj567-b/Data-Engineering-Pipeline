# ETL Pipeline

This project implements an ETL pipeline that extracts data from an API, transforms it using Apache Spark (PySpark), and loads the processed data into an Azure SQL database. The pipeline is scheduled to run monthly at midnight (12:00 AM) on the first day of the month using Apache Airflow.

## Project Architecture

The architecture of this project is as follows:

1. **Data Extraction (Extract)**: 
   - The pipeline uses the OMDb API to fetch movie data in JSON format.
   - The data is then saved locally as raw JSON files.
   
2. **Data Transformation (Transform)**:
   - PySpark is used to load the raw JSON files.
   - The data is cleaned, transformed, and aggregated as per the requirements.
   
3. **Data Loading (Load)**:
   - The transformed data is loaded into an Azure SQL database for further use.
   
4. **Scheduling (Batch Processing)**:
   - Apache Airflow is used to schedule and manage the execution of the pipeline.
   - The pipeline runs every month at 12:00 AM on the first day of the month.

## Prerequisites

Before running the pipeline, make sure you have the following prerequisites:

1. **Azure SQL Database**:
   - An Azure SQL database to store the transformed data.
   - Connection details such as the database host, username, password, and database name.

2. **Apache Airflow**:
   - Apache Airflow installed to schedule and manage the pipeline.
   - Make sure you have configured the Airflow scheduler and webserver properly.

3. **Python Libraries**:
   - Python 3.x (preferably 3.7+)
   - Install the necessary Python libraries using `pip`:
     ```bash
     pip install apache-airflow pyspark requests pyodbc
     ```

4. **Azure CLI**:
   - Azure CLI installed and configured to access your Azure account.


## Setup Process

### Step 1: Clone the Repository

1. Clone the repository to your local machine:

   ```bash
   git clone <repository_url>
   ```

### Step 2: Install Required Libraries and Framework

1.Install the necessary Python libraries by running the following command:

   ```bash
   pip install apache-airflow pyspark requests pyodbc
   ```
2. Install the ODBC Driver for SQL Server:

      ```bash
      sudo apt-get install unixodbc-dev
      sudo apt-get install msodbcsql17
      ```
### Step 3: Make the Necessory changes in Code

1.Change API_KEY = "Your API KEY and BASE_URL = "API URL" from Data_Extraction.py
2.Change Azure SQL Database credentials:
   server = 'Your Server Name'  # Azure SQL Server name
   database = 'Your DB Name'  # Database name
   username = 'Your User name'  # Database username
   password = 'Your Password'  # Database password
   driver = '{ODBC Driver 17 for SQL Server}'

### Step 3: Setup Airflow

1.Initialize Airflow Database:
   ```bash
      airflow db init
   ```
2.Start the Airflow Web Server:
   ```bash
      airflow webserver --port 8080
   ```
3.Start the Airflow Scheduler:
   ```bash
      airflow scheduler
   ```
4.Monitor the DAG
 
 Go to http://localhost:8080 to check the status of your DAG and monitor the execution of the tasks.


