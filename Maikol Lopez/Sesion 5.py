# Databricks notebook source
from pyspark.sql.types import *

ruta_ejercicio = 'dbfs:/FileStore/data/DATA_EJERCICIO_S4_CB.csv'

schema = StructType([
    StructField("ID", IntegerType(), True), \
    StructField("ID_1", IntegerType(), True), \
    StructField("NIVEL_EDUCATIVO", StringType(), True), \
    StructField("SEXO", StringType(), True), \
    StructField("CATEGORIA_EMPLEO", StringType(), True), \
    StructField("EXPERIENCIA_LABORAL", IntegerType(), True), \
    StructField("ESTADO_CIVIL", StringType(), True), \
    StructField("EDAD", IntegerType(), True), \
    StructField("UTILIZACION_TARJETAS", IntegerType(), True), \
    StructField("NUMERO_ENTIDADES", StringType(), True), \
    StructField("DEFAULT", StringType(), True), \
    StructField("NUM_ENT_W", FloatType(), True), \
    StructField("EDUC_W", FloatType(), True), \
    StructField("EXP_LAB_W", FloatType(), True), \
    StructField("EDAD_W", FloatType(), True), \
    StructField("UTIL_TC_W", FloatType(), True), \
    StructField("PD", FloatType(), True), \
    StructField("RPD", FloatType(), True), \
])

df_schema = spark.read.option("header",True).schema(schema).csv(ruta_ejercicio, sep = ";")

# COMMAND ----------

df_schema = df_schema.drop("ID_1")
df_schema.display()

# COMMAND ----------

from pyspark.sql import SQLContext

sc = spark.sparkContext

# COMMAND ----------

#Inicializar la instancia de SQLContext 

sqlContext = SQLContext(sc)

# COMMAND ----------

sqlContext.registerDataFrameAsTable(df_schema, "df_schema")

# COMMAND ----------

sqlContext.sql(" select ID, SEXO from df_schema ").display()

# COMMAND ----------

sqlContext.sql("""
               SELECT NIVEL_EDUCATIVO FROM
               df_schema GROUP BY NIVEL_EDUCATIVO
               """).display()

# COMMAND ----------

sqlContext.sql("""
               SELECT NIVEL_EDUCATIVO, AVG(EDAD) AS Promedio, count(*) AS Total FROM
               df_schema 
               GROUP BY NIVEL_EDUCATIVO
               ORDER BY Total
               """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC EJERCICIO
# MAGIC
# MAGIC Elaborar una consulta donde se muestren los clientes mayores a 35 años con menores probabilidades de default (los 5 primeros).
# MAGIC
# MAGIC Elaborar una consulta donde se muestren los solteros menores a 35 años que utilizan más de 3 veces su tc y tienen menor probabilidad de default (10 primeros)

# COMMAND ----------

sqlContext.sql("""
               SELECT ID
               FROM df_schema
               WHERE EDAD > 35
               AND PD = 0
               LIMIT 5
               """).display()

# COMMAND ----------

sqlContext.sql("""
               SELECT ID
               FROM df_schema
               WHERE ESTADO_CIVIL ='SOLTERO'
               AND EDAD < 35
               AND UTILIZACION_TARJETAS >= 3
               AND PD = 1
               LIMIT 10
               """).display()

# COMMAND ----------

#USO DE JOINS

data_heroes = [
    ('Deadpool', 3),
    ('Iron Man', 1),
    ('Groot', 7),
]

data_race = [
    ('Kryptoniano', 5),
    ('Mutante', 3),
    ('Humano', 1),
]

heroes = spark.createDataFrame(data_heroes, ["name","id"])
races = spark.createDataFrame(data_race, ["race","id"])

heroes.display()
races.display()

# COMMAND ----------

sqlContext.registerDataFrameAsTable(heroes,"heroes")
sqlContext.registerDataFrameAsTable(races,"races")

# COMMAND ----------

#Hacemos JOIN entre las 2 tablas creadas

sqlContext.sql("""
               select * from heroes a
               inner join races b on a.id = b.id
               """).display()

# COMMAND ----------

#Funciones con pyspark

df_schema.count()

# COMMAND ----------

df_schema.groupBy('SEXO').count().show()

# COMMAND ----------

import pyspark.sql.functions as f

df_schema.groupBy('SEXO').agg(f.mean('EDAD')).show()

# COMMAND ----------

heroes.join(races, on = 'id', how='inner').show()

# COMMAND ----------

heroes.join(races, on = 'id', how='outer').show()

# COMMAND ----------

heroes.join(races, on = 'id', how='outer').groupBy('race').count().show()

# COMMAND ----------

df_schema.groupBy('SEXO').agg(f.mean('EDAD')).filter(f.col('SEXO').isin('F')).show()

