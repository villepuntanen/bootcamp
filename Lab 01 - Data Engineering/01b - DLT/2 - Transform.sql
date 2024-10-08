-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Tables with SQL
-- MAGIC
-- MAGIC This notebook uses SQL to declare Delta Live Tables. 
-- MAGIC
-- MAGIC [Complete documentation of DLT syntax is available here](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql).
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 1: Create Bronze table for Sales

-- COMMAND ----------

CREATE STREAMING TABLE bronze_sales
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze sales table with all transactions"
AS 
SELECT * 
FROM
cloud_files('/FileStore/tmp/${current_user_id}/datasets/sales/', "json") 

-- COMMAND ----------

CREATE STREAMING TABLE bronze_stores
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Information about stores"
AS 
SELECT *, case when id in ('SYD01', 'MEL01', 'BNE02', 'MEL02', 'PER01', 'CBR01') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end as country_code 
FROM  
cloud_files('/FileStore/tmp/${current_user_id}/datasets/stores/', 'json');

-- COMMAND ----------

-- This table is different - it gets data as part of CDC feed from our source system
CREATE STREAMING TABLE bronze_products
TBLPROPERTIES ("quality" = "cdc")
COMMENT "CDC records for our products dataset"
AS 
SELECT * FROM 
cloud_files('/FileStore/tmp/${current_user_id}/datasets/products_cdc/', "json") ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 2: Create a Silver table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Referencing Streaming Tables
-- MAGIC
-- MAGIC Queries against other DLT tables and views will always use the syntax `live.table_name`. At execution, the target database name will be substituted, allowing for easily migration of pipelines between DEV/QA/PROD environments.
-- MAGIC
-- MAGIC When referring to another streaming DLT table within a pipeline, use the `STREAM(live.table_name)` syntax to ensure incremental processing.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Quality Control with Constraint Clauses
-- MAGIC
-- MAGIC Data expectations are expressed as simple constraint clauses, which are essential where statements against a field in a table.
-- MAGIC
-- MAGIC Adding a constraint clause will always collect metrics on violations. If no `ON VIOLATION` clause is included, records violating the expectation will still be included.
-- MAGIC
-- MAGIC DLT currently supports the following options for the `ON VIOLATION` clause.
-- MAGIC
-- MAGIC | mode | behavior |
-- MAGIC | --- | --- |
-- MAGIC | `FAIL UPDATE` | 	Invalid records prevent the update from succeeding. Manual intervention is required before re-processing. |
-- MAGIC | `DROP ROW` | Invalid records are dropped before data is written to the target; failure is reported as a metrics for the dataset. |
-- MAGIC | `warn` | Invalid records are written to the target; failure is reported as a metric for the dataset |
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Silver Sales Tables

-- COMMAND ----------

CREATE STREAMING TABLE silver_sales_clean (
  CONSTRAINT `Location has to be 5 characters long` EXPECT (length(store_id) = 5),
  CONSTRAINT `Only CANCELED and COMPLETED transactions are allowed` EXPECT (order_state IN ('CANCELED', 'COMPLETED'))
) 
TBLPROPERTIES ("quality" = "silver")
COMMENT "Silver table with clean transaction records" AS
  SELECT
    id AS id,
    ts AS ts,
    store_id AS store_id,
    customer_id AS customer_id,
    store_id || "-" || cast(customer_id as string) AS unique_customer_id,
    order_source AS order_source,
    STATE AS order_state,
    sale_items AS sale_items
  FROM STREAM(live.bronze_sales)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_sales;

-- Use APPLY CHANGES INTO to keep only the most rec
APPLY CHANGES INTO LIVE.silver_sales
  FROM 
  stream(live.silver_sales_clean)
  KEYS (id)
  SEQUENCE BY ts;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Silver Stores Table

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_stores  (
  CONSTRAINT `Location has to be 5 characters long` EXPECT (length(id) = 5)
  )
  TBLPROPERTIES ("quality" = "silver")
AS
SELECT * from STREAM(live.bronze_stores)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Silver Products Table
-- MAGIC
-- MAGIC Our silver_products table will be tracking changes history by using SCD TYPE 2 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_products;

-- Use APPLY CHANGES INTO to keep only the history as well
APPLY CHANGES INTO LIVE.silver_products
  FROM 
  stream(live.bronze_products)
  KEYS (id)
  IGNORE NULL UPDATES
  APPLY AS DELETE WHEN _change_type = 'delete'
  SEQUENCE BY _change_timestamp
  COLUMNS  * EXCEPT (_change_type, _change_timestamp, _rescued_data)
  STORED AS SCD TYPE 2

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Step 3: Create Gold tables
-- MAGIC
-- MAGIC These tables will be used by your business users and will usually contain aggregated datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Gold table example

-- COMMAND ----------

CREATE Materialized View country_sales AS
SELECT 
  l.country_code, 
  count(distinct s.id) AS number_of_sales
FROM live.silver_sales s 
INNER JOIN live.silver_stores l ON s.store_id = l.id
GROUP BY l.country_code;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Hands On Task!
-- MAGIC
-- MAGIC Create 2 more gold tables that would be using any of the existing silver ones and check how they appear on your DLT pipeline
-- MAGIC
-- MAGIC
-- MAGIC ### Advanced option
-- MAGIC
-- MAGIC Create another gold table, but this time using python. Note - you will need to use a new notebook for it and later add it to your existing DLT pipeline.
-- MAGIC
-- MAGIC You can also create a silver_sales_item table with each row containing information about specific juice sold and get more insights about most popular combinations!

-- COMMAND ----------

CREATE Materialized View store_sales AS
SELECT 
  l.city, 
  count(distinct s.id) AS number_of_sales
FROM live.silver_sales s 
INNER JOIN live.silver_stores l ON s.store_id = l.id
GROUP BY l.city;
