# Databricks notebook source
# MAGIC %md
# MAGIC # Création du point de montage /mnt/container

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://container@ttcproject.blob.core.windows.net/",
  mount_point = "/mnt/container",
  extra_configs = {"fs.azure.account.key.ttcproject.blob.core.windows.net": 
                    dbutils.secrets.get(scope = "dtscope", 
                                        key = "ttc-project")})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Afficher les points de montage

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Afficher le dataset

# COMMAND ----------

spark = SparkSession.builder.master("local[1]").appName("zebi").getOrCreate()
df = spark.read.csv("/mnt/container/ttc-data.csv")
df.show()
