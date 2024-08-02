# Databricks notebook source
# Vamos a crear nuestra BD

db = "S09_DB6_Maikol"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

spark.sql(f"USE {db}")

# Preparar nuestra base de datos y configurar para habilitar DL

spark.sql(" SET spark.databricks.delta.formatCheck.enable = false ")
spark.sql(" SET spark.databricks.delta.properties.autoOptimize.optimizeWrite = true ")


# COMMAND ----------

# Declarar variables para la conexión con la base de datos postgre

driver = "org.postgresql.Driver"
database_host = "databricksdmc.postgres.database.azure.com"
database_port = "5432"
database_name = "northwind"
database_user = "grupo6_antony"
password = "Especializacion6"
table = "orders"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

# COMMAND ----------

#Leemos una tabla de postgre

sql_order = (spark.read.format("jdbc").option("driver",driver).option("url",url).option("dbtable",table).option("user",database_user).option("password",password).load())

# COMMAND ----------

sql_order.display()

# COMMAND ----------

tables = ["orders","customer_customer_demo","customers","employees", "order_details", "region","territories","suppliers"]

datafames = {}

for table in tables:
    options = {'driver':driver, "url":url,"dbtable":table,"user":database_user,"password":password }
    datafames[table] = spark.read.format("jdbc").options(**options).load()

# COMMAND ----------

datafames["orders"].display()

# COMMAND ----------

sql_order.write.format("delta").mode("overwrite").saveAsTable("S09_DB6_Maikol.orders")

# COMMAND ----------

for table in tables:
    options = {'driver':driver, "url":url,"dbtable":table,"user":database_user,"password":password }
    dt_table = spark.read.format("jdbc").options(**options).load()
    table = "S09_DB6_Maikol."+table
    dt_table.write.format("delta").mode("overwrite").saveAsTable(table)
