# IPL Data Pipeline

A production-grade ETL pipeline for processing IPL match data with PySpark, Google Cloud Storage, and BigQuery.

## 📌 Overview

This pipeline processes IPL match data from:
- Match information (teams, venues, results)
- Ball-by-ball delivery data

Key features:
- Incremental processing of new data
- 8 analytical transformations
- GCS → BigQuery → PySpark → BigQuery/GCS flow
- Production-ready with logging, validation, and watermark tracking

## 🛠️ Tech Stack

- **Core**: Python 3.8+
- **Processing**: PySpark 3.3
- **Cloud Services**:
  - Google Cloud Storage (GCS)
  - BigQuery
- **Utilities**:
  - Python-dotenv (configuration)
  - Pytest (testing)

## 📂 Project Structure

ipl-data-pipeline/
├── config/ # Configuration files
├── connectors/ # GCS and BigQuery connectors
├── data_models/ # Data schemas
├── etl/ # Pipeline components
│ ├── extract/ # Data extraction
│ ├── transform/ # All transformations
│ └── load/ # Data loading
|-- input_files/ # Contains input files for upload and testing
| |__ combined_data/ # All the data  
| |__ saperated_data/ # saperate data for each file will be stored here
├── utils/ # Helper functions
├── tests/ # Unit tests
├── .env # Environment variables
├── requirements.txt # Python dependencies
└── main.py # Pipeline entry point


## 🔧 Setup Instructions

### Prerequisites
- Google Cloud account with:
  - GCS bucket
  - BigQuery dataset
  - Service account credentials
- Python 3.8+
- Java 8 (for PySpark)

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Rehman980/IPL-DATA-PIPELINE.git
   cd IPL-DATA-PIPELINE

2. Create and activate virtual environment:
```
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate  
```