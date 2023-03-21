// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
// MAGIC 
// MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

//File location and type
val file_location = "/FileStore/tables/BTC_USD-2.csv"
val file_type = "csv"

// CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","

val col_schemas = StructType(
  Array( 
      StructField("Date",DateType,true),
      StructField("Open",DoubleType,true),
      StructField("High",DoubleType,true),
      StructField("Low",DoubleType,true),
      StructField("Close",DoubleType,true),
      StructField("Adj Close",DoubleType,true),
      StructField("Volume",LongType,true)
    )
)

// The applied options are for CSV files. For other file types, these will be ignored.
val BTC_USDdF = spark.read
.format(file_type)
.schema(col_schemas)
.option("header", first_row_is_header) 
.option("sep", delimiter) 
.load(file_location)
display(BTC_USDdF)

// COMMAND ----------

val BTC_USD_2_dF = BTC_USDdF
.withColumnRenamed("Close", "Closing Price (USD)")
.withColumnRenamed("Adj Close", "Adjusted Close (USD)")
.withColumnRenamed("High", "24h High (USD)")
.withColumnRenamed("Low", "24h Low (USD)")
.withColumnRenamed("Open", "24h Open (USD)")
display(BTC_USD_2_dF)

// COMMAND ----------

val BTC_USD_3_dF = BTC_USD_2_dF.withColumn("Currency",lit("BTC"))
display(BTC_USD_3_dF)
