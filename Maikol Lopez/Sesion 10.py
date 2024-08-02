# Databricks notebook source
# https://<databricks-instance>#secrets/createScope

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="scope_Postgre_Maikol")

# COMMAND ----------

password = dbutils.secrets.get(scope="scope_Postgre_Maikol", key="secreto-postgre")

# COMMAND ----------

print(password)

# COMMAND ----------

driver = "org.postgresql.Driver"
database_host = "databricksdmc.postgres.database.azure.com"
database_port = "5432"
database_name = "northwind"
database_user = "grupo6_antony"
table = "orders"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

# COMMAND ----------

sql_order = (spark.read.format("jdbc").option("driver",driver).option("url",url).option("dbtable",table).option("user",database_user).option("password",password).load())

# COMMAND ----------

sql_order.display()
