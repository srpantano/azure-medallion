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

spark.sql('create database testing')
