# Databricks notebook source
printi('Hello world')

# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://bronze@medallionsa1974.blob.core.windows.net',
    mount_point='/mnt/bronze',
    extra_configs = {'fs.azure.account.key.medallionsa1974.blob.core.windows.net': dbutils.secrets.get('databricksScope', 'storageAccountKey')}
)
