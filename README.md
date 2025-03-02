README.md
# Australian Company Data Pipeline

This project builds a data pipeline to collect, clean, and integrate Australian company website data from [Common Crawl](https://commoncrawl.org/) with business information from the [Australian Business Register (ABR)](https://data.gov.au/). The pipeline uses Apache Spark for data extraction and processing, Python for orchestration, and DBT for transformation and data quality testing, storing the integrated data in a PostgreSQL database.

## Project Overview

### Data Sources
- **Common Crawl**: Extracts Australian company websites (`.com.au` domains) with:
  - Website URL
  - Company Name (where available)
  - Company Industry (for a subset)
  - **Note**: Currently limited to processing 1 WARC file due to local system memory constraints (e.g., 6GB Spark memory allocation). Scaling to 200,000+ records requires a more robust environment (e.g., cloud-based Spark cluster).
- **ABR**: Enriches website data with:
  - Australian Business Number (ABN)
  - Company Name
  - Business Status
  - Entity Type
  - Postcode

### Pipeline Description
The pipeline consists of three main stages:
1. **Extraction & Processing with Spark**:
   - Downloads and processes Common Crawl WARC files and ABR CSV data.
   - Cleans data (e.g., removes duplicates by ABN, fills missing values) and writes to PostgreSQL (`aus_company_db`).
   - Technologies: Python, Apache Spark 3.5.0.
   - Rationale: Spark’s distributed processing handles large datasets efficiently, while Python provides flexibility for scripting.
2. **Transformation with DBT**:
   - Creates staging models (`stg_abr`, `stg_websites`, `website_abr_mapping`) for cleaned and normalized data.
   - Merges website and ABR data via a mapping table.
   - Includes a basic uniqueness test on ABN.
   - Technologies: DBT 1.7.0, PostgreSQL.
   - Rationale: DBT ensures consistent transformations and data quality with a SQL-based workflow.
3. **Storage**: 
   - Data is stored in a PostgreSQL database (`aus_company_db`, schema: `australian_companies`).

### Schema Design
The database schema in PostgreSQL (`aus_company_db.australian_companies`) is:
```sql
CREATE SCHEMA australian_companies;

CREATE TABLE australian_companies.abr_data (
    abn BIGINT PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    business_status VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    postcode VARCHAR(4),
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE australian_companies.websites (
    url VARCHAR(255) PRIMARY KEY,
    company_name VARCHAR(255),
    industry VARCHAR(255)
);

CREATE TABLE australian_companies.website_abr_mapping (
    url VARCHAR(255) REFERENCES australian_companies.websites(url),
    abn BIGINT REFERENCES australian_companies.abr_data(abn),
    PRIMARY KEY (url, abn)
);
Current Output
Common Crawl: 247 .com.au websites extracted from 1 WARC file (limited by local system capacity).
ABR: 2,185,147 company records processed.
DBT: Successfully created staging models (stg_abr, stg_websites, website_abr_mapping) with one test (unique_abn).
Setup Instructions
Prerequisites
Python 3.10+
PostgreSQL 12+ (running locally on localhost:5432)
Apache Spark 3.5.0 (download from spark.apache.org and extract to C:/Users/joshi/spark/spark-3.5.0-bin-hadoop3)
Git (for cloning the repository)

Environment Setup

cd australian-company-data-pipeline
Create a Virtual Environment:
python -m venv spark_env

Activate the Virtual Environment:
spark_env\Scripts\activate

Linux/Mac:
source spark_env/bin/activate

Install Dependencies:
Ensure requirements.txt is in the project root.
pip install -r requirements.txt

Set Up PostgreSQL:
Create the database and user:

CREATE DATABASE aus_company_db;
CREATE USER pipeline_user_new WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE aus_company_db TO pipeline_user_new;
Connect to aus_company_db and create the schema:
CREATE SCHEMA australian_companies;
GRANT ALL PRIVILEGES ON SCHEMA australian_companies TO pipeline_user_new;

Configure DBT:
Ensure ~/.dbt/profiles.yml exists:

aus_company_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: pipeline_user_new
      password: Ni@211203
      dbname: aus_company_db
      schema: australian_companies

Set Up Spark:
Ensure Spark is installed.
Update RUN_PIPELINE.PY if Spark path differs.

Running the Pipeline
Activate Environment:
spark_env\Scripts\activate

Run the Pipeline:
python RUN_PIPELINE.PY
Extracts Common Crawl and ABR data, processes with Spark, and runs DBT transformations.

Verify Output:
Check PostgreSQL:
\dt australian_companies.*
SELECT * FROM australian_companies.stg_abr LIMIT 5;

