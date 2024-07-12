# Databricks notebook source
df = spark.read.format('csv').option("header","true").load('/mnt/demo/data1.csv')
df.display()

# COMMAND ----------

# MAGIC %md Skip top N records in csv

# COMMAND ----------

start=10
path='/mnt/demo/data1.csv'
df = (spark.read.format('csv')
     .option("inferSchema", "true")
     .option("header","true")
     .option('skipRows',start)
     .load(path))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Skip specific range of records in csv

# COMMAND ----------

import pyspark.sql.functions as F
start=5
end=10
fulldf = (spark.read.format('csv')
     .option("inferSchema", "true")
     .option("header","true")
     .load(path))
skipStartdf = (spark.read.format('csv')
     .option("inferSchema", "true")
     .option("header","true")
     .option('skipRows',start)
     .load(path))
skipEnddf = (spark.read.format('csv')
     .option("inferSchema", "true")
     .option("header","true")
     .option('skipRows',end)
     .load(path))
diff = fulldf.subtract(skipStartdf)
final_df = diff.union(skipEnddf)
final_df.orderBy(F.col('deviceId').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Skip range of records in dataframe

# COMMAND ----------

df = spark.read.format('csv').option("header","true").load('/mnt/demo/data1.csv')

# COMMAND ----------

import pyspark.sql.functions as F
first_skip_rows = 5
last_skip_rows = 10
fulldf = df.withColumn('index', F.monotonically_increasing_id())
skipf = (fulldf.where((F.col('index')>= first_skip_rows) 
                    & (F.col('index')< last_skip_rows)))
finaldf = (fulldf.subtract(skipf)
          .orderBy(F.col('index')))
finaldf.display()
