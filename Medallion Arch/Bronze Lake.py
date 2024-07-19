# Databricks notebook source
# mounting the GCS bucket
bucket_name = "msklspace"
mount_name = "ayushi_ecom"
dbutils.fs.mount(
  f"gs://{bucket_name}",
  f"/mnt/databricks/{mount_name}",
  extra_configs = {"fs.gs.project.id": "mentorsko-1700569460412"}
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create bronze schema
# MAGIC create schema bronze;
# MAGIC use schema bronze;

# COMMAND ----------

# creating a list of all the tables (csv files) in the mounted bucket
csv_files = dbutils.fs.ls(f"dbfs:/mnt/{mount_name}")
Tables = [file.name[:-4] for file in csv_files if file.name.endswith('.csv')]
print(Tables)

# COMMAND ----------

# creating empty tables in the bronze layer for each csv file
for table in Tables:
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        -- Define your schema here
    )
    """
    spark.sql(create_table_query)
    
    #copying data from csv files to the bronze tables    
    copy_into_query = f"""
    COPY INTO {table}
    FROM 'dbfs:/mnt/{mount_name}/{table}.csv'
    FORMAT_OPTIONS("header" = "true", "inferSchema" = "true", "mergeSchema" = "true", "timestampFormat" = "dd-MM-yyyy HH.mm")
    COPY_OPTIONS("mergeSchema" = "true")
    """
    spark.sql(copy_into_query)
