import os
import shutil
import subprocess
import requests
from warcio import ArchiveIterator
from bs4 import BeautifulSoup
from io import BytesIO
import zipfile
from xml.etree import ElementTree as ET
import yaml
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import *  # Add this import for lit, current_timestamp, etc.
import pandas as pd
import json
import gc
import psycopg2
from psycopg2 import Error

# Configuration
DB_NAME = "aus_company_db"
DB_USER = "pipeline_user_new"
DB_PASSWORD = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_DIR, "data", "raw")
WARC_URL = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-42/segments/1727944253146.59/warc/CC-MAIN-20241003094020-20241003124020-00000.warc.gz"
ABR_URLS = [
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
]
PSQL_PATH = r"C:\Program Files\PostgreSQL\17\bin\psql.exe"
OUTPUT_DIR = r"C:\Users\joshi\hit2\common_crawl\2024-42"
ABR_OUTPUT_DIR = r"C:\Users\joshi\hit2\abr"

# Set up logging
logging.basicConfig(filename=os.path.join(OUTPUT_DIR, "warc_processing.log"), level=logging.WARNING, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Set Spark and Hadoop environment variables explicitly
os.environ["SPARK_HOME"] = r"C:\Users\joshi\spark\spark-3.5.0-bin-hadoop3"
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.26.4-hotspot"
os.environ["HADOOP_HOME"] = r"C:\Program Files\Hadoop"
ANACONDA_PYTHON = r"C:\Users\joshi\AppData\Local\anaconda3\envs\spark_env\python.exe"
os.environ["PYSPARK_PYTHON"] = ANACONDA_PYTHON
os.environ["HADOOP_OPTS"] = "-Djava.library.path=%HADOOP_HOME%\\bin"  # Help load native libs
os.environ["PATH"] = (
    f"{os.environ['JAVA_HOME']}\\bin;"
    f"{os.environ['SPARK_HOME']}\\bin;"
    f"{os.environ['HADOOP_HOME']}\\bin;"
    r"C:\Users\joshi\AppData\Local\anaconda3\envs\spark_env;"
    r"C:\Users\joshi\AppData\Local\anaconda3\envs\spark_env\Scripts;"
    f"{os.environ.get('PATH', '')}"
)

def run_command(command, shell=True):
    """Run a command and handle errors."""
    try:
        result = subprocess.run(command, shell=shell, check=True, text=True, capture_output=True)
        print(result.stdout)
        if result.stderr:
            print(f"Error Output: {result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        print(f"Error Output: {e.stderr}")
        exit(1)

def setup_environment():
    """Install Python dependencies using Anaconda's virtual env Python."""
    print("Setting up environment...")
    with open("requirements.txt", "w") as f:
        f.write("requests==2.31.0\nwarcio==1.7.4\nbeautifulsoup4==4.12.2\ndbt-postgres==1.7.0\nPyYAML==6.0.1\nsetuptools==70.0.0\npyspark==3.5.0")
    run_command(f"\"{ANACONDA_PYTHON}\" -m pip install -r requirements.txt")

def setup_postgres():
    """Configure PostgreSQL database and schema with a new role."""
    print("Setting up PostgreSQL...")
    os.makedirs(os.path.join(PROJECT_DIR, "sql"), exist_ok=True)
    
    os.environ["PGPASSWORD"] = "password"
    psql_cmd = f"\"{PSQL_PATH}\" -U postgres -h {DB_HOST} -p {DB_PORT}"
    
    run_command(f"{psql_cmd} -c \"DROP DATABASE IF EXISTS {DB_NAME};\"")
    run_command(f"{psql_cmd} -c \"DROP ROLE IF EXISTS {DB_USER};\"")
    
    run_command(f"{psql_cmd} -c \"CREATE ROLE {DB_USER} WITH LOGIN PASSWORD '{DB_PASSWORD}';\"")
    run_command(f"{psql_cmd} -c \"ALTER ROLE {DB_USER} CREATEDB;\"")
    run_command(f"{psql_cmd} -c \"CREATE DATABASE {DB_NAME} OWNER {DB_USER};\"")
    
    run_command(f"{psql_cmd} -c \"DROP ROLE IF EXISTS reader;\"")
    run_command(f"{psql_cmd} -c \"CREATE ROLE reader WITH NOLOGIN;\"")
    
    schema_sql = """
    CREATE SCHEMA australian_companies;

    CREATE TABLE australian_companies.websites (
        website_id SERIAL PRIMARY KEY,
        url VARCHAR(255) NOT NULL UNIQUE,
        company_name VARCHAR(255),
        industry VARCHAR(100),
        crawl_date DATE,
        extracted_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE australian_companies.abr_data (
        abn VARCHAR(11) PRIMARY KEY,
        company_name VARCHAR(255) NOT NULL,
        business_status VARCHAR(50),
        entity_type VARCHAR(50),
        postcode VARCHAR(4),
        updated_at TIMESTAMP
    );

    CREATE TABLE australian_companies.website_abr_mapping (
        mapping_id SERIAL PRIMARY KEY,
        website_id INT REFERENCES australian_companies.websites(website_id),
        abn VARCHAR(11) REFERENCES australian_companies.abr_data(abn),
        confidence_score FLOAT,
        UNIQUE (website_id, abn)
    );

    GRANT USAGE ON SCHEMA australian_companies TO reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA australian_companies TO reader;
    """
    with open(os.path.join(PROJECT_DIR, "sql", "schema.sql"), "w") as f:
        f.write(schema_sql)
    
    os.environ["PGPASSWORD"] = DB_PASSWORD
    run_command(f"\"{PSQL_PATH}\" -U {DB_USER} -h {DB_HOST} -p {DB_PORT} -d {DB_NAME} -f \"{os.path.join(PROJECT_DIR, 'sql', 'schema.sql')}\"")

def setup_dbt():
    """Set up DBT."""
    print("Setting up DBT...")
    dbt_dir = os.path.join(PROJECT_DIR, "dbt_project")
    os.makedirs(os.path.join(dbt_dir, "models", "staging"), exist_ok=True)
    os.makedirs(os.path.join(dbt_dir, "models", "marts"), exist_ok=True)
    os.makedirs(os.path.join(dbt_dir, "tests"), exist_ok=True)

    dbt_project_yaml = {
        "name": "aus_company_pipeline",
        "version": "1.0.0",
        "config-version": 2,
        "profile": "default",
        "model-paths": ["models"],
        "test-paths": ["tests"],
        "target-path": "target",
        "clean-targets": ["target", "dbt_packages"],
        "models": {
            "aus_company_pipeline": {
                "staging": {"materialized": "table"},
                "marts": {"materialized": "table"}
            }
        }
    }
    with open(os.path.join(dbt_dir, "dbt_project.yml"), "w") as f:
        yaml.dump(dbt_project_yaml, f)

    sources_yaml = {
        "version": 2,
        "sources": [{
            "name": "raw",
            "database": DB_NAME,
            "schema": "australian_companies",
            "tables": [
                {"name": "common_crawl", "external": {"location": r"C:\Users\joshi\hit2\common_crawl\2024-42", "format": "csv"}},
                {"name": "abr", "external": {"location": r"C:\Users\joshi\hit2\abr", "format": "csv"}}
            ]
        }]
    }
    with open(os.path.join(dbt_dir, "sources.yml"), "w") as f:
        yaml.dump(sources_yaml, f)

    profiles_dir = os.path.expanduser("~/.dbt")
    os.makedirs(profiles_dir, exist_ok=True)
    profiles_yaml = {
        "default": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "postgres",
                    "host": DB_HOST,
                    "user": DB_USER,
                    "password": DB_PASSWORD,
                    "port": int(DB_PORT),
                    "dbname": DB_NAME,
                    "schema": "australian_companies"
                }
            }
        }
    }
    with open(os.path.join(profiles_dir, "profiles.yml"), "w") as f:
        yaml.dump(profiles_yaml, f)

def fetch_warc_urls(crawl="CC-MAIN-2024-42", domain="*.com.au", limit=50):
    """Fetch WARC URLs containing .com.au domains from Common Crawl Index."""
    print(f"Fetching up to {limit} WARC URLs for {domain} from {crawl}...")
    index_url = f"http://index.commoncrawl.org/{crawl}-index?url={domain}&output=json&limit={limit}"
    headers = {'User-Agent': 'cc-extractor/1.0 (joshi@example.com)'}
    try:
        response = requests.get(index_url, headers=headers, timeout=30)
        response.raise_for_status()
        records = [json.loads(line) for line in response.text.strip().split('\n')]
        warc_urls = [f"https://data.commoncrawl.org/{r['filename']}" for r in records]
        print(f"Fetched {len(warc_urls)} WARC URLs")
        return warc_urls
    except requests.RequestException as e:
        print(f"Index query failed: {e}")
        return []

def download_warc_files(warc_urls, base_dir=OUTPUT_DIR):
    """Download WARC files to the specified directory."""
    os.makedirs(base_dir, exist_ok=True)
    downloaded_files = []
    headers = {'User-Agent': 'cc-extractor/1.0 (joshi@example.com)'}
    for url in warc_urls:
        filename = os.path.join(base_dir, url.split('/')[-1])
        print(f"Downloading {url} to {filename}")
        try:
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded {filename}")
            downloaded_files.append(filename)
        except requests.RequestException as e:
            print(f"Failed to download {url}: {e}")
    return downloaded_files

def cleanup_temp_dir(temp_dir, retries=5, delay=5):
    """Retry deleting a temp directory to handle Windows file locking issues."""
    for attempt in range(retries):
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                print(f"Successfully deleted temp dir: {temp_dir}")
            break
        except Exception as e:
            print(f"Failed to delete {temp_dir} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)  # Wait longer before retrying
            else:
                print(f"Could not delete {temp_dir} after {retries} attempts. Manual cleanup may be required.")

def extract_common_crawl():
    print("Extracting Common Crawl data...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("CommonCrawlExtract") \
        .master("local[2]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.network.timeout", "600s") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    temp_dir = spark.sparkContext._temp_dir  # Capture temp dir for cleanup
    
    warc_urls = fetch_warc_urls(limit=1)
    warc_files = download_warc_files(warc_urls)
    existing_warc = os.path.join(OUTPUT_DIR, "CC-MAIN-20241003094020-20241003124020-00000.warc.gz")
    if os.path.exists(existing_warc):
        warc_files.append(existing_warc)
    
    records = []
    for warc_file in warc_files:
        print(f"Processing WARC file: {warc_file}")
        try:
            with open(warc_file, "rb") as stream:
                for record in ArchiveIterator(stream):
                    if record.rec_type == "response":
                        uri = record.rec_headers.get("WARC-Target-URI")
                        if uri and ".com.au" in uri:
                            html = record.content_stream().read().decode("utf-8", errors="ignore")
                            soup = BeautifulSoup(html, "html.parser")
                            name = soup.title.string.strip() if soup.title else None
                            industry = soup.find('meta', {'name': 'industry'})['content'] if soup.find('meta', {'name': 'industry'}) else None
                            records.append((uri, name, industry))
        except Exception as e:
            print(f"Error processing {warc_file}: {e}")
    
    schema = StructType([
        StructField("url", StringType(), False),
        StructField("company_name", StringType(), True),
        StructField("industry", StringType(), True)
    ])
    df = spark.createDataFrame(records, schema)
    abs_output_dir = os.path.join(OUTPUT_DIR, "extracted_websites.parquet")
    df.write.mode("overwrite").parquet(abs_output_dir)
    
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/aus_company_db") \
        .option("dbtable", "australian_companies.websites") \
        .option("user", "pipeline_user_new") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()
    
    row_count = df.count()
    print(f"Processed {row_count} .com.au records to {abs_output_dir} and Postgres")
    
    # Cleanup
    spark.stop()
    gc.collect()  # Force garbage collection to release file handles
    cleanup_temp_dir(temp_dir)  # Retry deleting temp dir

def extract_abr_data():
    print("Extracting ASIC Companies data...")
    output_dir = r"C:\Users\joshi\hit2\asic_companies"
    csv_url = "https://data.gov.au/data/dataset/asic-companies/resource/5c3914e6-413e-4a2c-b890-bf8efe3eabf2/download/company_202206.csv"
    
    os.makedirs(output_dir, exist_ok=True)
    print(f"Created directory: {output_dir}")

    spark = SparkSession.builder \
        .appName("ASICDataExtract") \
        .master("local[2]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.network.timeout", "600s") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    temp_dir = spark.sparkContext._temp_dir  # Capture temp dir for cleanup
    print(f"Spark Version: {spark.version}")

    temp_csv = os.path.join(output_dir, "temp_company.csv")
    try:
        # Download the CSV
        response = requests.get(csv_url, stream=True, timeout=30)
        response.raise_for_status()
        with open(temp_csv, "wb") as f:
            f.write(response.content)

        # Read CSV into DataFrame
        df = spark.read.option("header", "true") \
                       .option("delimiter", "\t") \
                       .option("quote", "\"") \
                       .option("escape", "\"") \
                       .option("multiLine", "true") \
                       .option("inferSchema", "true") \
                       .csv(temp_csv)
        
        print("CSV Schema:")
        df.printSchema()
        
        print("First 5 rows:")
        df.show(5, truncate=False)
        
        # Clean and transform the DataFrame
        df_cleaned = df.dropDuplicates(["ABN"]).na.fill({
            "Company Name": "", 
            "Status": "Unknown", 
            "Type": "Unknown"
        }).select(
            "ABN",
            "Company Name",
            "Status",
            "Type"
        ).withColumnRenamed("Company Name", "company_name") \
         .withColumnRenamed("Status", "business_status") \
         .withColumnRenamed("Type", "entity_type") \
         .withColumnRenamed("ABN", "abn") \
         .withColumn("postcode", lit(None).cast(StringType())) \
         .withColumn("updated_at", current_timestamp())

        df_cleaned.cache()
        row_count = df_cleaned.count()
        print(f"Cleaned DataFrame has {row_count} rows")
        
        # Save to Parquet
        abs_output_dir = os.path.abspath(output_dir).replace("\\", "/")
        df_cleaned.write.mode("overwrite").parquet(abs_output_dir)
        
        # Define JDBC connection properties
        jdbc_url = "jdbc:postgresql://localhost:5432/aus_company_db"
        properties = {
            "user": "pipeline_user_new",
            "password": "password",
            "driver": "org.postgresql.Driver"
        }
        
        # Drop the table with CASCADE using psycopg2
        drop_query = "DROP TABLE IF EXISTS australian_companies.abr_data CASCADE;"
        try:
            connection = psycopg2.connect(
                dbname="aus_company_db",
                user=properties["user"],
                password=properties["password"],
                host="localhost",
                port="5432"
            )
            cursor = connection.cursor()
            cursor.execute(drop_query)
            connection.commit()
            print("Successfully dropped table australian_companies.abr_data with CASCADE")
        except Error as e:
            print(f"Error dropping table: {e}")
            raise
        finally:
            if connection:
                cursor.close()
                connection.close()
        
        # Write cleaned DataFrame to PostgreSQL
        df_cleaned.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "australian_companies.abr_data") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .option("stringtype", "unspecified") \
            .option("batchsize", "10000") \
            .mode("append") \
            .save()
        
        print(f"Processed {row_count} ASIC records to {abs_output_dir} and Postgres")
        df_cleaned.unpersist()

    except Exception as e:
        print(f"Failed during Spark processing: {e}")
        raise
    finally:
        if os.path.exists(temp_csv):
            os.remove(temp_csv)
            print(f"Removed temporary CSV: {temp_csv}")
        # Enhanced cleanup
        spark.sparkContext._jsc.stop()  # Explicitly stop JVM context
        spark.stop()
        gc.collect()  # Force garbage collection
        cleanup_temp_dir(temp_dir, retries=5, delay=5)  # More retries, longer delay

def run_dbt():
    """Run DBT transformations."""
    print("Running DBT...")
    os.chdir(os.path.join(PROJECT_DIR, "dbt_project"))
    run_command("dbt debug")
    run_command("dbt run")
    run_command("dbt test")

def main():
    """Main function to run the pipeline."""
    setup_environment()
    setup_postgres()
    setup_dbt()
    extract_common_crawl()  # Now fully functional
    extract_abr_data()
    run_dbt()
    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()