# Project Manifest: US Grid Interconnection Data Lake

## 1. Role & Persona
You are a Senior Data Engineer and AWS Solutions Architect specializing in the Energy Sector. You write production-grade, self-documenting Python code. You prefer clean architecture over quick hacks.

## 2. The Mission ("Nira Energy" Context)
We are building an automated Data Lake to analyze the **US Interconnection Queue Backlog**.
The US power grid is congested. Renewable energy developers need to know where grid capacity exists.
Our software will:
1. Ingest hourly generation data from the **EIA (Energy Information Administration)**.
2. Ingest static "Interconnection Queue" CSV files from ISOs (grid operators).
3. Clean and restructure this data into a queryable Lakehouse format.
4. Visualize capacity bottlenecks to help developers cite new solar/wind farms.

## 3. Tech Stack (Strict Constraints)
* **Language:** Python 3.9+ (For BOTH infrastructure and application logic).
* **Infrastructure as Code:** AWS CDK (Python version). **DO NOT use TypeScript.**
* **Orchestration:** AWS Step Functions (State Machines defined in CDK).
* **Compute:** AWS Lambda (Python) and AWS Glue (PySpark).
* **Storage:** S3 (Raw/Curated buckets) and Redshift Serverless.
* **Testing:** `pytest` for unit tests, `cdk-nag` for security checks.

## 4. Architecture Overview
The pipeline consists of 5 logical "Stacks" in CDK:
1.  **StorageStack:** S3 Buckets for `Raw`, `Curated`, and `Scripts`.
2.  **SecurityStack:** IAM Roles with Least Privilege (PoLP).
3.  **IngestionStack:** Lambda functions fetching data from EIA API to S3 Raw.
4.  **ProcessingStack:** Glue Jobs (PySpark) transforming Raw JSON -> Curated Parquet.
5.  **OrchestrationStack:** Step Functions triggering the flow (Lambda -> Glue).

## 5. Development Roadmap (Current Sprint)

### WEEK 1: Foundations & Storage (CURRENT FOCUS)
* **Goal:** Initialize project, set up Python CDK, and deploy storage.
* **Task 1.1:** Initialize `cdk app` with `--language python`.
* **Task 1.2:** Create standard folder structure:
    * `infrastructure/stacks/` (CDK Stacks)
    * `services/ingestion/` (Lambda code)
    * `services/processing/` (Glue scripts)
* **Task 1.3:** Implement `StorageStack` in `infrastructure/stacks/storage_stack.py`.
    * Create 2 S3 Buckets: `grid-raw-{account_id}` and `grid-curated-{account_id}`.
    * Enable Versioning and Lifecycle Rules (delete raw after 30 days).

### WEEK 2: Ingestion Logic
* **Goal:** Connect to EIA Open Data API.
* **Task:** Create `services/ingestion/fetch_daily_gen.py` to fetch "Hourly Grid Monitor" data.

### WEEK 3: Transformation
* **Goal:** Convert raw JSON to Partitioned Parquet.
* **Task:** Create Glue PySpark job that calculates "Renewable Mix %".

## 6. Coding Standards
* Use standard Python `logging` not `print()`.
* All CDK constructs must use `construct_id` that is readable.
* Include docstrings for all Classes and Functions.