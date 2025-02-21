# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "316a267f-948d-4685-9915-afcf2bd59851",
# META       "default_lakehouse_name": "workinglh",
# META       "default_lakehouse_workspace_id": "077cd378-40a5-44c8-9714-207c8be826f0"
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format("csv").option("header", "true").load("Files/bronze/sales.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("salesorders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
