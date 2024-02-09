# Data-Engineering-Chicago-Taxi
Data Pipelines with different technologies (BigQuery, Airflow, DBT, CloudBuild, others) for Chicago Taxi Dataset


## Workflow

### Data Production
Downloading the data from the City of Chicago's public dataset and uploading it to Google Cloud Storage.
Segregated by year and month in GCS folders

### Data Ingestion
Loading the data from GCS to BigQuery

### Data Modeling
Transforming the data in BigQuery using SQL and DBT models

## Enhancements
- Use cosmos (Open Source) to be able to view logs and task of the data models pipeline from airflow (instead of dbt UI)
- Having separate environments for development and production, connecting the `master` branch to the production environment and the `develop` branch to the development environment for the whole services (Airflow Composer, DBT, BigQuery, GCS, CloudBuild, etc.)
- Incremental load of data from GCS to BigQuery and fill the data models incrementally using dbt
- Using job tags in dbt to be able to run relevant models together
- Implement tests in dbt
- Implement Data quality checks (Great Expectations, SODA, etc..)
- Repo enhancements (PR checks, contribution process, CODEOWNERS, etc..)
- Others


## Note
cloudbuild is implemnted in `cloudbuild.yaml` and also from GCP side, but you'll see the builds are failing due to quota issues with GCP.
Composer Is Running Well