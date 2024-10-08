-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Tables with SQL
-- MAGIC
-- MAGIC This notebook uses SQL to declare Delta Live Tables. 
-- MAGIC
-- MAGIC [Complete documentation of DLT syntax is available here](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic DLT SQL Syntax
-- MAGIC
-- MAGIC At its simplest, you can think of DLT SQL as a slight modification to tradtional CTAS statements.
-- MAGIC
-- MAGIC DLT tables and views will always be preceded by the `LIVE` keyword.
-- MAGIC
-- MAGIC If you wish to process data incrementally (using the same processing model as Structured Streaming), also use the `STREAMING` keyword.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Step 1: Create Bronze table for Sales

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_sales
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Bronze sales table with all transactions"
AS 
SELECT * 
FROM
cloud_files( '/FileStore/tmp/apjdatabricksbootcamp/datasets/sales/' , "json") 

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_stores
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Store locations dimension"
AS 
SELECT *, case when id in ('SYD01', 'MEL01', 'BNE02', 'MEL02', 'PER01', 'CBR01') then 'AUS' when id in ('AKL01', 'AKL02', 'WLG01') then 'NZL' end as country_code 
FROM  
cloud_files('/FileStore/tmp/apjdatabricksbootcamp/datasets/stores/' , 'json');

-- COMMAND ----------

-- This table is different - it gets data as part of CDC feed from our source system
CREATE STREAMING LIVE TABLE bronze_products
TBLPROPERTIES ("quality" = "cdc")
COMMENT "CDC records for our products dataset"
AS 
SELECT * FROM 
cloud_files( '/FileStore/tmp/apjdatabricksbootcamp/datasets/products_cdc/' , "json") ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Weather data
-- MAGIC
-- MAGIC We also have some data from weather API - it can be transformed using a different notebook, but we can also add it here.
-- MAGIC
-- MAGIC **Important**
-- MAGIC
-- MAGIC Change data path to match one you had in `01 - Ingest notebook` to pick up your dataset. You can get it by running cell below

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC current_user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
-- MAGIC weather_files_location = f"/FileStore/tmp/{current_user_id}/datasets/weather/"
-- MAGIC print(weather_files_location)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_weather
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Records from weather api"
AS 
SELECT * FROM 
cloud_files( '/FileStore/tmp/apjdatabricksbootcamp/datasets/weather/' , "json") ;

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
-- MAGIC DLT currently supports two options for the `ON VIOLATION` clause.
-- MAGIC
-- MAGIC | mode | behavior |
-- MAGIC | --- | --- |
-- MAGIC | `FAIL UPDATE` | Fail when expectation is not met |
-- MAGIC | `DROP ROW` | Only process records that fulfill expectations |
-- MAGIC | ` ` | Alert, but still process |
-- MAGIC
-- MAGIC
-- MAGIC Roadmap: `QUARANTINE`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Silver fact tables

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_sales_clean (
  CONSTRAINT `Location has to be 5 characters long` EXPECT (length(store_id) = 5),
  CONSTRAINT `Only CANCELED and COMPLETED transactions are allowed` EXPECT (order_state IN ('CANCELED', 'COMPLETED'))
) 
TBLPROPERTIES ("quality" = "silver")
COMMENT "Silver table with clean transaction records" AS
  SELECT
    id as id,
    ts as ts,
    store_id as store_id,
    customer_id as customer_id,
    store_id || "-" || cast(customer_id as string) as unique_customer_id,
    order_source as order_source,
    STATE as order_state,
    sale_items as sale_items
  from STREAM(live.bronze_sales)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_sales;

-- Use APPLY CHANGES INTO to keep only the most rec
APPLY CHANGES INTO LIVE.silver_sales
  FROM 
  stream(live.silver_sales_clean)
  KEYS (id)
  SEQUENCE BY ts


-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_sales_items_clean (
  CONSTRAINT `All custom juice must have ingredients` EXPECT (
    NOT(
      product_id = 'Custom'
      and product_ingredients is null
    )
  )
) 
TBLPROPERTIES ("quality" = "silver") 
COMMENT "Silver table with clean transaction records" 
AS
SELECT
  id || "-" || cast(pos as string) as id,
  id as sale_id,
  ts as sale_ts,
  store_id,
  pos as item_number,
  col.id as product_id,
  col.size as product_size,
  col.notes as product_notes,
  col.cost as product_cost,
  col.ingredients as product_ingredients
from
  (
    select
      *,
      posexplode(
        from_json(
          sale_items,
          'ARRAY<STRUCT<cost: STRING, id: STRING, ingredients: STRING, notes: STRING, size: STRING>>'
        )
      )
    from
      (
        SELECT
          id as id,
          ts as ts,
          store_id as store_id,
          customer_id as customer_id,
          store_id || "-" || cast(customer_id as string) as unique_customer_id,
          order_source as order_source,
          STATE as order_state,
          sale_items as sale_items
        from
          STREAM(live.bronze_sales)
      )
  )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_sales_items;

APPLY CHANGES INTO LIVE.silver_sales_items
  FROM STREAM(live.silver_sales_items_clean)
  KEYS (id)
  SEQUENCE BY (sale_ts, item_number)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dimension tables in Silver layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Silver layer is a good place to add some data quality expectations

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
-- MAGIC Weather data needs some cleanup to convert it to multiple rows

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_weather_clean AS
select
  concat_ws(latitude, longitude, t.time) as pk,
  latitude,
  longitude,
  timezone,
  generationtime_ms,
  t.time,
  t.temperature_2m,
  t.rain
from
  (
    select
      latitude,
      longitude,
      timezone,
      generationtime_ms,
      explode(
        arrays_zip(hourly.time, hourly.temperature_2m, hourly.rain) 
      ) as t
    from
      (
        select
          latitude,
          longitude,
          timezone,
          generationtime_ms,
          from_json(
            hourly,
            schema_of_json(
              '{"rain": [0, 0.1, 0.1], "temperature_2m": [21.9, 21.6, 21], "time": ["2023-03-01T00:00", "2023-03-01T01:00", "2023-03-01T02:00"]}'
            )
          ) as hourly
        from
          STREAM(live.bronze_weather)
      )
  )

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Hands On Task!
-- MAGIC
-- MAGIC Finish the cell below to have a silver_weather table with unique and up to date records by using APPLY CHANGES INTO and using table `live.silver_weather_clean`
-- MAGIC
-- MAGIC For this table we do not need to track history - keep it as SCD Type 1

-- COMMAND ----------

-- REMOVE COMMENT AND FINISH WRITING THIS SQL

CREATE OR REFRESH STREAMING LIVE TABLE silver_weather;

APPLY CHANGES INTO LIVE.silver_weather FROM 
 stream(live.silver_weather_clean)
  KEYS (pk)
  IGNORE NULL UPDATES
  SEQUENCE BY generationtime_ms
  STORED AS SCD TYPE 1

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## Step 3: Create Gold tables
-- MAGIC
-- MAGIC These tables will be used by your business users and will usually contain aggregated datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Enrich dataset with lookup table

-- COMMAND ----------

CREATE LIVE TABLE country_sales
select l.country_code, count(distinct s.id) as number_of_sales
from live.silver_sales s 
  join live.silver_stores l on s.store_id = l.id
group by l.country_code;

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
-- MAGIC Create another gold table, but this time using python. Note - you will need to use a new notebook for it and later add it to your existing DLT pipeline

-- COMMAND ----------

CREATE LIVE TABLE daily_sales_and_weather with weather as (
  select
    date_trunc('day', time) as day,
    avg(temperature_2m) as average_temperature
  from
    live.silver_weather
  where
    temperature_2m is not null
  group by
    1
),
sales as (
  select
    date_trunc('day', sale_ts) as day,
    avg(total_sale_cost) as average_daily_cost,
    sum(total_sale_cost) as total_daily_cost
  from
    (
      select
        sale_ts,
        sale_id,
        sum(product_cost) as total_sale_cost
      from
        live.silver_sales_items
      group by
        1,
        2
    )
    group by 1
)
select
  sales.*,
  weather.average_temperature
from
  sales
  left join weather on sales.day = weather.day
