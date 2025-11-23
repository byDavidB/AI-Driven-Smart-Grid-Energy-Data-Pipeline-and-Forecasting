# US Grid Interconnection Data Lake

## Overview
This project aims to build an automated Data Lake to analyze the **US Interconnection Queue Backlog**. With the US power grid becoming increasingly congested, renewable energy developers need accurate data on where grid capacity exists.

This software will:
1. Ingest hourly generation data from the **EIA (Energy Information Administration)**.
2. Ingest static "Interconnection Queue" CSV files from ISOs (grid operators).
3. Clean and restructure this data into a queryable Lakehouse format.
4. Visualize capacity bottlenecks to help developers cite new solar/wind farms.

## Tech Stack
* **Language:** Python 3.9+ (Infrastructure & Application)
* **Infrastructure as Code:** AWS CDK (Python)
* **Orchestration:** AWS Step Functions
* **Compute:** AWS Lambda (Python) and AWS Glue (PySpark)
* **Storage:** S3 (Raw/Curated buckets) and Redshift Serverless
* **Testing:** `pytest`, `cdk-nag`

## Architecture
The pipeline is organized into 5 logical CDK Stacks:
1. **StorageStack:** S3 Buckets for `Raw`, `Curated`, and `Scripts`.
2. **SecurityStack:** IAM Roles with Least Privilege (PoLP).
3. **IngestionStack:** Lambda functions fetching data from EIA API to S3 Raw.
4. **ProcessingStack:** Glue Jobs (PySpark) transforming Raw JSON -> Curated Parquet.
5. **OrchestrationStack:** Step Functions triggering the flow (Lambda -> Glue).

## Development Roadmap

### WEEK 1: Foundations & Storage
* Initialize project and set up Python CDK.
* Deploy storage infrastructure (S3 Buckets).

### WEEK 2: Ingestion Logic
* Connect to EIA Open Data API.
* Implement `fetch_daily_gen.py` to fetch "Hourly Grid Monitor" data.

### WEEK 3: Transformation
* Convert raw JSON to Partitioned Parquet.
* Create Glue PySpark job to calculate "Renewable Mix %".

## Getting Started
This project uses AWS CDK with Python.

### Prerequisites
* Python 3.9+
* AWS CLI configured
* AWS CDK Toolkit installed

### Initialization
To initialize the virtual environment:

```bash
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### Deployment
To deploy the stacks:

```bash
cdk deploy --all
```
