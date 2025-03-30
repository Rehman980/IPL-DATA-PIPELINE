# IPL Data Engineering Pipeline

![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)

A production-ready data pipeline for processing Indian Premier League (IPL) match data using PySpark, Google Cloud Storage (GCS), and BigQuery.

## Features

- **Data Ingestion**: Read raw match data from GCS (CSV/Parquet)
- **Data Quality Checks**: Validate schema, null values, and business rules
- **Transformations**: PySpark-based processing with star schema output
- **Warehousing**: Load processed data into BigQuery
- **GCS Integration**: Store final outputs in partitioned Parquet files
- **Isolated Environments**: Separate service accounts for multi-project safety

## Prerequisites

### Hardware/OS
- Windows 10/11 or Linux
- 8GB+ RAM (16GB recommended)
- 10GB free disk space

### Software
1. [Python 3.8+](https://www.python.org/downloads/)
2. [Java JDK 8+](https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html)
3. [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
4. (Optional) [VS Code](https://code.visualstudio.com/) with Python extension

## Setup Guide

### 1. GCP Configuration
1. Create a new project in [Google Cloud Console](https://console.cloud.google.com/)
2. Enable these APIs:
   - BigQuery API
   - Cloud Storage API
3. Create a service account with these roles:
   - BigQuery Data Editor
   - Storage Object Admin
   - Storage Admin
   - BigQuery Job User

### 2. Local Environment Setup
```bash
# Clone repository
git clone https://github.com/your-username/ipl-data-pipeline.git
cd ipl-data-pipeline

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Set up environment variables (create .env file)
echo "GCP_PROJECT=your-project-id" > .env
echo "GCS_BUCKET=your-bucket-name" >> .env
echo "GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json" >> .env


# If any error occurs while creating spark session like: Exception in thread "main" java.lang.RuntimeException: [unresolved dependency: com.google.cloud.spark#spark-bigquery-with-dependencies_2.12;0.28.0: not found]

1. Download the JAR file:

   * Go to Maven Repository

   * Download version 0.28.0 Direct link: [here](spark-bigquery-with-dependencies_2.12-0.28.0.jar)

   * Save it to C:\spark_jars\

2. Modify your Spark session creation in main.py:
   spark = (SparkSession.builder
         .appName("IPL Data Processing")
         .config("spark.jars", "C:\\spark_jars\\spark-bigquery-with-dependencies_2.12-0.28.0.jar")
         .getOrCreate())