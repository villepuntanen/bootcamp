# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Show Lineage for Delta Tables in Unity Catalog
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/uc-lineage-slide.png" style="float:right; margin-left:10px" width="700"/>
# MAGIC
# MAGIC Unity Catalog captures runtime data lineage for any table to table operation executed on a Databricks cluster or SQL endpoint. Lineage operates across all languages (SQL, Python, Scala and R) and it can be visualized in the Data Explorer in near-real-time, and also retrieved via REST API.
# MAGIC
# MAGIC Lineage is available at two granularity levels:
# MAGIC - Tables
# MAGIC - Columns: ideal to track GDPR dependencies
# MAGIC
# MAGIC Lineage takes into account the Table ACLs present in Unity Catalog. If a user is not allowed to see a table at a certain point of the graph, its information are redacted, but they can still see that a upstream or downstream table is present.
# MAGIC
# MAGIC ## Working with Lineage
# MAGIC
# MAGIC No modifications are needed to the existing code to generate the lineage. As long as you operate with tables saved in the Unity Catalog, Databricks will capture all lineage informations for you.
# MAGIC
# MAGIC Requirements:
# MAGIC - Make sure you set `spark.databricks.dataLineage.enabled true`in your cluster setup
# MAGIC - Source and target tables must be registered in a Unity Catalog metastore to be eligible for lineage capture
# MAGIC - The data manipulation must be performed using Spark DataFrame language (python/SQL)
# MAGIC - To view lineage, users must have the SELECT privilege on the table
# MAGIC
# MAGIC <!-- tracking, please do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fuc%2Flineage%2Flineage&dt=FEATURE_UC_LINAGE">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Cluster setup for UC
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-cluster-setup-single-user.png" style="float: right"/>
# MAGIC
# MAGIC
# MAGIC To be able to run this demo, make sure you create a cluster with the security mode enabled.
# MAGIC
# MAGIC Go in the compute page, create a new cluster.
# MAGIC
# MAGIC Select "Single User" and your UC-user (the user needs to exist at the workspace and the account level)

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md ## 1/ Create a Delta Table In Unity Catalog
# MAGIC
# MAGIC The first step is to create a Delta Table in Unity Catalog.
# MAGIC
# MAGIC We want to do that in SQL, to show multi-language support:
# MAGIC
# MAGIC 1. Use the `CREATE TABLE` command and define a schema
# MAGIC 1. Use the `INSERT INTO` command to insert some rows in the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_CATALOG();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS field_demosAPS.uc_lineage.menu (recipe_id INT, app string, main string, desert string);
# MAGIC DELETE from uc_lineage.menu ;
# MAGIC
# MAGIC INSERT INTO uc_lineage.menu 
# MAGIC     (recipe_id, app, main, desert) 
# MAGIC VALUES 
# MAGIC     (1,"Ceviche", "Tacos", "Flan"),
# MAGIC     (2,"Tomato Soup", "Souffle", "Creme Brulee"),
# MAGIC     (3,"Chips","Grilled Cheese","Cheescake");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe extended uc_lineage.menu

# COMMAND ----------

# MAGIC %md-sandbox ## 2/ Create a Delta Table from the Previously Created One
# MAGIC
# MAGIC To show dependancies between tables, we create a new one `AS SELECT` from the previous one, concatenating three columns into a new one

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uc_lineage.dinner 
# MAGIC   AS SELECT recipe_id, concat(app," + ", main," + ",desert) as full_menu FROM uc_lineage.menu

# COMMAND ----------

# MAGIC %md-sandbox ## 3/ Create a Delta Table as join from Two Other Tables
# MAGIC
# MAGIC The last step is to create a third table as a join from the two previous ones. This time we will use Python instead of SQL.
# MAGIC
# MAGIC - We create a Dataframe with some random data formatted according to two columns, `id` and `recipe_id`
# MAGIC - We save this Dataframe as a new table, `main.lineage.price`
# MAGIC - We read as two Dataframes the previous two tables, `main.lineage.dinner` and `main.lineage.price`
# MAGIC - We join them on `recipe_id` and save the result as a new Delta table `main.lineage.dinner_price`

# COMMAND ----------

df = spark.range(3).withColumn("price", F.round(10*F.rand(seed=42),2)).withColumnRenamed("id", "recipe_id")

df.write.mode("overwrite").saveAsTable("uc_lineage.price")

dinner = spark.read.table("uc_lineage.dinner")
price = spark.read.table("uc_lineage.price")

dinner_price = dinner.join(price, on="recipe_id")
dinner_price.write.mode("overwrite").saveAsTable("uc_lineage.dinner_price")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Visualize Table Lineage
# MAGIC
# MAGIC The Lineage can be visualized in the `Data Explorer` of the part of the Workspace dedicated to the `SQL Persona`.
# MAGIC
# MAGIC 1. Select the `Catalog`
# MAGIC 1. Select the `Schema`
# MAGIC 1. Select the `Table`
# MAGIC 1. Select the `Lineage` tab on the right part of the page
# MAGIC 1. You can visualize the full lineage by pressing the `See Lineage Graph` button
# MAGIC 1. By default the graph is condensed. By clicking on the boxes you can expand them and visualize the full lineage.
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/lineage-table.gif"/>

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 5/ Visualize Column Lineage
# MAGIC
# MAGIC The Lineage is alos available for the Column. This is very useful to track column dependencies and be able to find GDPR, including by API.
# MAGIC
# MAGIC You can access the column lineage by clicking on any of the column name. In this case we see that the menu comes from 3 other columns of the menu table:
# MAGIC <br/><br/>
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/lineage-column.gif"/>

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 6/ Lineage Permission Model
# MAGIC
# MAGIC Lineage graphs share the same permission model as Unity Catalog. If a user does not have the SELECT privilege on the table, they will not be able to explore the lineage.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Conclusion
# MAGIC
# MAGIC Databricks Unity Catalog let you track data lineage out of the box.
# MAGIC
# MAGIC No extra setup required, just read and write from your table and the engine will build the dependencies for you. Lineage can work at a table level but also at the column level, which provide a powerful tool to track dependencies on sensible data.
# MAGIC
# MAGIC Lineage can also show you the potential impact updating a table/column and find who will be impacted downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Existing Limitations
# MAGIC - Streaming operations are not yet supported
# MAGIC - Lineage will not be captured when data is written directly to files in cloud storage even if a table is defined at that location (eg spark.write.save(“s3:/mybucket/mytable/”) will not produce lineage)
# MAGIC - Lineage is not captured across workspaces (eg if a table A > table B transformation is performed in workspace 1 and table B > table C in workspace 2, each workspace will show a partial view of the lineage for table B)
# MAGIC - Lineage is computed on a 30 day rolling window, meaning that lineage will not be displayed for tables that have not been modified in more than 30 days ago
