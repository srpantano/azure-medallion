# azure-medallion

## Project using Azure Datafactory, Databricks, and DBT in a medallion architecture. 

First, a DataFactory pipeline fetch all tables in a database and copy one by one to parquet files on a 'bronze' Data Lake container.
