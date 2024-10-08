# Databricks notebook source
current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
datasets_location = f'/FileStore/tmp/{current_user_id}/datasets/'

dbutils.fs.rm(datasets_location, True)
print(f'Deleted data files from location: %s' %datasets_location)

# COMMAND ----------

database_name = current_user_id.split('@')[0].replace('.','_')+'_bootcamp'
spark.sql(f'drop database if exists {database_name} cascade;')
print(f'Deleted database: %s' %database_name)
