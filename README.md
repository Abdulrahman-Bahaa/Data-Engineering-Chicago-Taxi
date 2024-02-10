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
- Implement tests in dbt
- Implement Data quality checks (Great Expectations, SODA, etc..)
- Repo enhancements (PR checks, contribution process, CODEOWNERS, etc..)
- Add airflow Variables file to the repo and connect it with cloudbuild
- Implement Reporting Layer in BigQuery, to be able to drive better insights and performance
- Others


## Note
cloudbuild step for installing requirements to the environment is implemented and working but gives an error when no updates for the packages exist. 
The error is not critical and the pipeline continues to work as expected, and it can be suppressed by adding checking the output of the bash command in the cloudbuild step and ignoring if the output is this issue `No change in configuration. Must specify a change to configuration.software_configuration.pypi_dependencies`
This is ignored for now as it's not used in the current project setup, no custom packages required to be installed in the environment.