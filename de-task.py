# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog asgmnt_test_catlg;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze managed location 
# MAGIC 'abfss://assignments@westeudestorage.dfs.core.windows.net/bronze';
# MAGIC create schema if not exists silver managed location
# MAGIC 'abfss://assignments@westeudestorage.dfs.core.windows.net/silver';
# MAGIC create schema if not exists gold managed location
# MAGIC 'abfss://assignments@westeudestorage.dfs.core.windows.net/gold';
# MAGIC
# MAGIC create schema if not exists etl_meta managed location
# MAGIC 'abfss://assignments@westeudestorage.dfs.core.windows.net/etl_meta';
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
input_path = "abfss://assignments@westeudestorage.dfs.core.windows.net/input/coil_input.csv"
archive_path = "abfss://assignments@westeudestorage.dfs.core.windows.net/archive"
catalog_nm = "asgmnt_test_catlg"


# COMMAND ----------

# %sql
# drop table if exists asgmnt_test_catlg.bronze.coil_raw;
# drop table if exists asgmnt_test_catlg.silver.coil_stg;
# drop table if exists asgmnt_test_catlg.silver.coil_invalid_stg;
# drop table if exists asgmnt_test_catlg.gold.coil_summary;
# drop table if exists asgmnt_test_catlg.etl_meta.audit_table;

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, count, avg, max, sum, expr, to_date,current_timestamp,lit


def log_audit_entry(status, message):
    run_timestamp = datetime.now()
    batch_id = "batch_id_" + str(datetime.now())
    audit_entry = [(batch_id, run_timestamp, status, message)]
    print(audit_entry)
    audit_df = spark.createDataFrame(audit_entry, ["batch_id", "run_timestamp", "status", "message"])
    audit_df.write.mode('overwrite').saveAsTable(f"{catalog_nm}.etl_meta.audit_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load input file into raw table

# COMMAND ----------

try:
    if dbutils.fs.ls(input_path):
        input_df = spark.read.format("csv").option("header", "true")\
            .option("inferSchema", "true")\
            .load(f"{input_path}")\
            .withColumn('insert_date', current_timestamp())
        input_df.write.mode('append').saveAsTable(f'{catalog_nm}.bronze.coil_raw') 

        # Move files to archive folder
        dbutils.fs.mv(input_path, archive_path, True)
    else:
        raise FileNotFoundError(f"The specified path {input_path} does not exist.")
except Exception as e:
    print(e)
    log_audit_entry("FAILED", "Pipeline execution failed to ingest data into raw table.")
    



# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the data and load valid records into the Silver table

# COMMAND ----------

# audit_df = spark.read.table("config.audit_table")
# audit_df.show()

# COMMAND ----------

from pyspark.sql.functions import *
if spark._jsparkSession.catalog().tableExists(f"{catalog_nm}.etl_meta.audit_table"):
    audit_df = spark.read.table(f"{catalog_nm}.etl_meta.audit_table")
    last_run_timestamp = audit_df.filter(col("status") == "SUCCESS").agg({"run_timestamp": "max"}).collect()[0][0]
    audit_df.show()



# COMMAND ----------

# DBTITLE 1,Load to the silver table
try:
    raw_df = spark.read.table('bronze.coil_raw')
    if last_run_timestamp is not None:
       raw_df = raw_df .filter(col('insert_date') > last_run_timestamp)
    cleaned_df = raw_df.filter((col("coil_id").isNotNull()) &
                                (col("production_date").isNotNull()) &
                                (col("material_type").isNotNull()) &
                                 ((col("weight").isNotNull())|(col("weight")>0)) &
                                 (col("thickness").isNotNull())).distinct()
                            

    invalid_df = raw_df.filter((col("coil_id").isNull()) |
                                (col("production_date").isNull()) |
                                (col("material_type").isNull())|
                                (col("weight").isNull())| (col("weight")<=0) |
                                col("thickness").isNull())
    cleaned_df.write.mode('append').saveAsTable('silver.coil_stg')
    invalid_df.write.mode('append').saveAsTable('silver.coil_invalid_stg')
except Exception as e:
    print(e)
    log_audit_entry("FAILED", "Pipeline execution failed to load silver table.")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a summary table to capture the quality of manufacturing products

# COMMAND ----------



# Handle None for last_run_timestamp
if last_run_timestamp is None:
    last_run_timestamp = "1990-01-01"

try:
    last_run_timestamp = to_timestamp(lit(last_run_timestamp))

    # Filter the DataFrame and perform aggregations
    output_df = (
        spark.table("silver.coil_stg")
        .filter(
            (col("insert_date") > last_run_timestamp)
            & (col("weight") > 1000)
            & (col("thickness") >= 1.0)
        )
        .groupBy("material_type", "production_date")
        .agg(
            count("coil_id").alias("total_coils"),
            avg("weight").alias("avg_weight"),
            avg("thickness").alias("avg_thickness"),
            max("line_speed").alias("max_line_speed"),
            sum("defects").alias("total_defects"),
            (sum("defects") / count("coil_id") * 100).alias("defect_rate"),
        )
    )
    output_df.createOrReplaceTempView("output_df_temp")

    if spark._jsparkSession.catalog().tableExists(f"{catalog_nm}.default.audit_table"):
        merge_query = """
   MERGE INTO asgmnt_test_catlg.gold.coil_summary AS target
   USING output_df_temp AS source
    ON target.production_date = source.production_date AND target.material_type = source.material_type
   WHEN MATCHED THEN
    UPDATE SET
    target.total_coils = source.total_coils,
    target.avg_weight = source.avg_weight,
    target.avg_thickness = source.avg_thickness,
    target.max_line_speed = source.max_line_speed,
    target.total_defects = source.total_defects,
    target.defect_rate = source.defect_rate
   WHEN NOT MATCHED THEN
    INSERT (material_type, production_date, total_coils, avg_weight, avg_thickness, max_line_speed, total_defects, defect_rate)
   VALUES (source.material_type, source.production_date, source.total_coils, source.avg_weight, source.avg_thickness, source.max_line_speed, source.total_defects, source.defect_rate)
    """
        spark.sql(merge_query)
    else:
        output_df.write.mode("append").saveAsTable(
            "asgmnt_test_catlg.gold.coil_summary"
        )
except exception as e:
    log_audit_entry("FAILED", "Pipeline execution failed to load silver table.")
    dbutils.notebook.exit(1)


log_audit_entry("SUCCESS", "The Pipeline is executed successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from asgmnt_test_catlg.gold.coil_summary;
# MAGIC select * from asgmnt_test_catlg.etl_meta.audit_table;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA asgmnt_test_catlg.bronze TO developer_grp;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA asgmnt_test_catlg.silver TO developer_grp;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA asgmnt_test_catlg.gold TO developer_grp;
# MAGIC
# MAGIC GRANT SELECT ON SCHEMA asgmnt_test_catlg.bronze TO support_L2_grp;
# MAGIC GRANT SELECT ON SCHEMA asgmnt_test_catlg.silver TO support_L2_grp;
# MAGIC GRANT SELECT ON SCHEMA asgmnt_test_catlg.gold TO support_L2_grp;
# MAGIC
# MAGIC GRANT SELECT ON SCHEMA asgmnt_test_catlg.gold TO analyst_grp;