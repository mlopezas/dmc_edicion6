# Databricks notebook source
# DBTITLE 1,Importar librerias
#Voy a instalar una librería

!pip install pyspark

# COMMAND ----------

dbutils.library.restartPython()

# !pip install NOMBRE_LIBRERIA

# COMMAND ----------

# MAGIC %md
# MAGIC GRUPO DE TEXTO

# COMMAND ----------

# DBTITLE 1,AÑADIR LIBRERIA A SCRIPT
import pyspark
import pandas as pd

# COMMAND ----------

spark.read.table('Ventas_maikol')

# COMMAND ----------

spark

# COMMAND ----------

df_ventas = spark.read.table("ventas_maikol")

# COMMAND ----------

df_ventas.printSchema()

# COMMAND ----------

df_ventas.show()

# COMMAND ----------

df_ventas.display()

# COMMAND ----------

df_ventas.select("Tienda","Género").show()

# COMMAND ----------

# Agregar una nueva columna

df_ventas = df_ventas.withColumn("Venta_USD", df_ventas["precio_de_venta S/"]/3.71)

# COMMAND ----------

df_ventas.display()

# COMMAND ----------

#EJERCIO

#Cargar el archivo "Ejecicio_Venta"
#Crear una columna (Precio Total) -> cantidad * precio
#Crear una columna (Impuesto), que va a ser el 18% del precio total

# COMMAND ----------

df_ventas_ejercicio = spark.read.table("ventas_ejercicio_maikol")

# COMMAND ----------

df_ventas_ejercicio.display()

# COMMAND ----------

df_ventas_ejercicio = df_ventas_ejercicio.withColumn("Precio_Total", df_ventas_ejercicio["cantidad"]*df_ventas_ejercicio["precio"])

#df_ventas = df_ventas.withColumn("Venta_USD", df_ventas["precio_de_venta S/"]/3.71)

# COMMAND ----------

df_ventas_ejercicio.display()

# COMMAND ----------

df_ventas_ejercicio = df_ventas_ejercicio.withColumn("Impuesto", df_ventas_ejercicio["Precio_Total"]/0.18)

# COMMAND ----------

df_ventas_ejercicio.display()
