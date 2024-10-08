# Databricks notebook source
catalog_name = 'tbd_ville_test'
current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'
spark.sql(f'use catalog {catalog_name};')



datasets_location = f'/FileStore/tmp/{current_user_id}/datasets/'

dbutils.fs.rm(datasets_location, True)

# COMMAND ----------

database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'
spark.sql(f'drop database if exists {database_name} cascade;')

# COMMAND ----------


