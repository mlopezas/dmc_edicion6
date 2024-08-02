# Databricks notebook source
# RDD (Resilient Distributed Dataset)
# Un RDD es una colección inmutable de objetos distribuidos que podemos procesar en paralelo.


#Caracteristicas:
#-Inmutable
#-Tolerancia a fallos # rdd1 -> rdd2 --> rdd3 - DAG
#-Particionado

#Operaciones -> Transformaciones - Acciones

#Datos No Estructurados

# COMMAND ----------

# DBTITLE 1,Crear un RDD
lista = ['AWS','Azure','GCP','HUAWEI','ON PREMIAE']

# COMMAND ----------

print(type(lista))

# COMMAND ----------

# MAGIC %md
# MAGIC Nota 1:
# MAGIC
# MAGIC Para aplicaciones en producción, vamos a usar principalmente RDD utilizando sistemas de almacenamiento externo. Por ejemplo: HDFS, S3, HBASE, ETC.
# MAGIC Parallelize, es una función en park que nos eprmite crear un RDD a partir de una colección de listas.

# COMMAND ----------

rdd = spark.sparkContext.parallelize(lista)

# COMMAND ----------

type(rdd)

# COMMAND ----------

df_park = spark.read.table("ventas_maikol")

# COMMAND ----------

type(df_park)

# COMMAND ----------

rdd2 = df_park.rdd

# COMMAND ----------

df_park.unpersist()

# COMMAND ----------

type(rdd2)

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Función Map()
# MAGIC
# MAGIC La función map() es transformar datos en rdd de Apache Spark. Las modificiaciones que hagamos se van a almacenar sobre otro espacio en la memoria.
# MAGIC
# MAGIC ¿Qué hace?
# MAGIC
# MAGIC Toma como entrada cada elemento de un RDD. Aplica una función definida por el usuario. Genera una salida transformada.

# COMMAND ----------

# MAGIC %md
# MAGIC Otras Funciones:
# MAGIC
# MAGIC ReduceByKey(): Fusionar claves para formar una cadena. Simil a concat SQL.
# MAGIC
# MAGIC SorByKey(): Simil a Order By en SQL.
# MAGIC
# MAGIC Filter(): Un filtro en RDD
# MAGIC
# MAGIC First(): Devulve el primer registro.
# MAGIC
# MAGIC Max(): Devolver el valor maximo.
# MAGIC
# MAGIC Reduce(): Reduce los a uno solo, y puedo hacer operaciones. Simil a Group By

# COMMAND ----------

# MAGIC %md
# MAGIC DATAFRAME
# MAGIC
# MAGIC Dataset organizado por columnas. Colección de datos con una estructura definida. A diferencia de los rdd, aquí podemos trabajar con información estructurada. Fuentes de datos pueden ser txt, csv, parquet, xml. etc.

# COMMAND ----------

#Creamos una lista

data = [("AWS", 20000),("Azure",50000),("GCP", 150000),("On Premise", 800000)]

# COMMAND ----------

type(data)

# COMMAND ----------

rdd3 = spark.sparkContext.parallelize(data)

# COMMAND ----------

rdd3.collect()

# COMMAND ----------

#Convertir un RDD a un DF

df_from_rdd = rdd3.toDF()

# COMMAND ----------

type(df_from_rdd)

# COMMAND ----------

df_from_rdd.printSchema()

# COMMAND ----------

columnas = ["Nombre_Nube","Num_Usuarios"]


df_from_rdd = rdd3.toDF(columnas)

# COMMAND ----------

df_from_rdd.printSchema()

# COMMAND ----------

#Crear DF con esquema en base a una 2 listas

data = [("AWS", 20000),("Azure",50000),("GCP", 150000),("On Premise", 800000)]

columnas = ["Nombre_Nube","Num_Usuarios"]


df_from_data = spark.createDataFrame(data).toDF(*columnas)

# COMMAND ----------

df_from_data.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Creando un Dataframe con Esquema
# MAGIC
# MAGIC En caso necesitemos crear un dataframe y necesitemos especificar los tipos de datos, nombre de columnas y si se admite nulos, vamos a usar un schema StructType y este esquema lo vamos a asignar al df
# MAGIC
# MAGIC Esquema: Formato: String, Double, etc.

# COMMAND ----------

#Vamos a importar librerias para el uso de esquemas

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

#Creamos una lista

data2 = [("Abel","Malpartida", 78546323, 150000.0),("Luis","Marengo", 99546323, 150000.0),("Maikol","Lopez", 77746323, 150000.0),("Erick","Gonzales", 44446323, 150000.0)]

# COMMAND ----------

schema = StructType([
    StructField("Nombre", StringType(), True),
    StructField("Apellido", StringType(), True),
    StructField("DNI", StringType(), False),
    StructField("Salario", FloatType(), True),
])

# COMMAND ----------

df = spark.createDataFrame(data = data2, schema= schema)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import *

ruta = "dbfs:/FileStore/data/VENTAS_EJERCICIO.txt"

#Definir esquema

schema = StructType([
    StructField("CODIGO", IntegerType(), True),
    StructField("PRODUCTO", StringType(), True),
    StructField("CLIENTE", StringType(), True),
    StructField("CANTIDAD", FloatType(), True),
    StructField("PRECIO", FloatType(), True),
])

# COMMAND ----------

df_with_schema = spark.read.format("csv").option("header","true").option("delimiter",",").schema(schema).load(ruta)

# COMMAND ----------

df_with_schema.display()

# COMMAND ----------

#Opción 1

df_with_schema.select("CODIGO", "CLIENTE").show()

# COMMAND ----------

#Opción 2

df_with_schema.select(df_with_schema.CODIGO, df_with_schema.CLIENTE).show()

# COMMAND ----------

#Opción 3

#Opción 2df_with_schema.select(df_with_schema["CODIGO"], df_with_schema["CLIENTE"]).show()

# COMMAND ----------

from pyspark.sql.functions import col


df_with_schema.select(col("CODIGO"),col("CLIENTE")).display()

# COMMAND ----------

#WithColumn()

#Función que nos permite modificar data, interactuar y crear columnas.

#Casteo de variables

df_with_schema.withColumn("PRECIO", col("PRECIO").cast("Float")).printSchema()

# COMMAND ----------

df_with_schema = df_with_schema.withColumn("PRECIO", col("PRECIO") * 1.2)

# COMMAND ----------

df_with_schema.show()

# COMMAND ----------

df_with_schema = df_with_schema.withColumn("PRECIO_IGV", col("PRECIO") * 1.8)

# COMMAND ----------

df_with_schema.show()

# COMMAND ----------

df_with_schema = df_with_schema.drop("PRECIO_IGV")

# COMMAND ----------

df_with_schema.show()
