# Databricks notebook source
#dbutils.fs.mount(
#    source='wasbs://bronze@medallionsa1974.blob.core.windows.net',
#    mount_point='/mnt/bronze',
#    extra_configs={'fs.azure.account.key.medallionsa1974.blob.core.windows.net': dbutils.secrets.get('databricksScope', 'storageAccountKey')}
#)

#dbutils.fs.mount(
#    source='wasbs://silver@medallionsa1974.blob.core.windows.net',
#    mount_point='/mnt/silver',
#    extra_configs={'fs.azure.account.key.medallionsa1974.blob.core.windows.net': dbutils.secrets.get('databricksScope', 'storageAccountKey')}
#)

#dbutils.fs.mount(
#    source='wasbs://gold@medallionsa1974.blob.core.windows.net',
#    mount_point='/mnt/gold',
#    extra_configs={'fs.azure.account.key.medallionsa1974.blob.core.windows.net': dbutils.secrets.get('databricksScope', 'storageAccountKey')}
#)

# COMMAND ----------

#dbutils.fs.ls('/mnt/bronze/2024-08-12/SalesLT.Customer.parquet')

# COMMAND ----------

fileName = dbutils.widgets.get('fileName')
tableSchema = dbutils.widgets.get('table_schema')
tableName = dbutils.widgets.get('table_name')

# COMMAND ----------

#create database if it does not exists
spark.sql(f'create database if not exists {tableSchema}')

# COMMAND ----------

filePath = f'/mnt/bronze/{fileName}'
df = spark.read.format('parquet').load(filePath)
df.write.mode('overwrite').saveAsTable(f'{tableSchema}.{tableName}')
