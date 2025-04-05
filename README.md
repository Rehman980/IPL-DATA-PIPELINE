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
```
ipl-data-pipeline/
├── config/ # Configuration files
├── connectors/ # GCS and BigQuery connectors
├── data_models/ # Data schemas
├── etl/ # Pipeline components
│ ├── extract/ # Data extraction
│ ├── transform/ # All transformations
│ └── load/ # Data loading
|input_files/ # Contains input files for upload and testing
| ├── combined_data/ # All the matches data
| └── saperated_data/ # saperate files for each match will be stored here to upload one-by-one in gcs
├── utils/ # Helper functions
├── tests/ # Unit tests
├── .env # Environment variables
├── requirements.txt # Python dependencies
└── main.py # Pipeline entry point
```

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
  ```bash
  python -m venv venv
  source venv/bin/activate  # Linux/Mac
  venv\Scripts\activate  
  ```
3. Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```
4. Set up environment:
   * create a .env file in the root directory
   * Copy .env example to .env
   * Update with your GCP credentials:
      ```ini
      GCP_PROJECT=your-project-id
      GCS_BUCKET=your-bucket-name
      BQ_DATASET=ipl_analytics
      GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
      ```
 5. Create testing input files:
       * Run the below given command to create saperate input files for each month
           ```bash
           python .\input_files\input_files_saperator.py
           ```
       * locate the 1st match's files and upload them into yous gcs bucket's "raw/matches/" and "raw/deliveries/" folders
       * Repeat this steps with different match's data after each execution of the pipeline to test incremental load and analytics
     
  ## 🚀 Running the Pipeline
  Execute the main pipeline:
  ```bash
  python main.py
  ```

  ## 🔍 Analytical Transformations
  1. Team Performance
      * Matches played/won
      * Average runs scored/conceded
      * Win percentages

  2. Batsman Statistics
      * Runs, average, strike rate
      * 4s/6s count
      * Dismissal analysis

  3. Bowler Statistics
      * Wickets taken
      * Economy rate
      * Bowling average

  4. Toss Impact Analysis
      * Toss decision outcomes
      * Win percentage by toss decision

  5. Player of Match Analysis
      * Most awards
      * Performance by season

  6. Death Overs Analysis
      * Runs scored/conceded (overs 16-20)
      * Wicket patterns

  7. Phase Comparison
      * Powerplay (1-6) vs Middle (7-15) vs Death (16-20)
      * Run rate comparison

  8. Venue Analysis
      * Pitch behavior
      * Average scores
      * Win patterns

  ## 🧪 Testing
  Run unit tests:
  ```bash
  pytest tests/
  ```
  
  ## 📊 Output Destinations
  * BigQuery Tables:
    * team_metrics
    * batsman_metrics
    * bowler_metrics
    * toss_analysis
    * pom_analysis
    * death_over_stats
    * match_phase_stats
    * venue_metrics
  * GCS Exports:
    * CSV files in gs://your-bucket/analytics/[metric_type]/[date].csv

  ## 🔄 Incremental Processing
  The pipeline automatically:
  * Tracks processed files using BigQuery watermark table
  * Only processes new data in each run
  * Maintains complete history in staging tables

  ## 📧 Contact
  For questions or support, please contact rahmanali980@gmail.com.
