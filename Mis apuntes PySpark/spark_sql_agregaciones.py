# -*- coding: utf-8 -*-
"""Spark_SQL_agregaciones.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1MFlW3i32zZ_pEEv3oIn1BI6f7nNlysbC
"""

# Instalar SDK Java 8

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# Descargar Spark 3.2.2

!wget -q https://archive.apache.org/dist/spark/spark-3.2.3/spark-3.2.3-bin-hadoop3.2.tgz

# Descomprimir el archivo descargado de Spark

!tar xf spark-3.2.3-bin-hadoop3.2.tgz

# Establecer las variables de entorno

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.2.3-bin-hadoop3.2"

# Instalar la librería findspark 

!pip install -q findspark

# Instalar pyspark

!pip install -q pyspark

# Explorando los datos


import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/dataframe.parquet')


df.printSchema()

df.show(20, truncate=False)

# Funciones count, countDistinct y approx_count_distinct

df = spark.read.parquet('./data/dataframe.parquet')

df.printSchema()

df.show()

# count

from pyspark.sql.functions import count

df.select(
    count('nombre').alias('conteo_nombre'),
    count('color').alias('conteo_color')
).show()

#los valores null no se cuentan

df.select(
    count('nombre').alias('conteo_nombre'),
    count('color').alias('conteo_color'),
    count('*').alias('conteo_general')
).show()

# countDistinct

from pyspark.sql.functions import countDistinct

df.select(
    countDistinct('color').alias('colores_dif')
).show()

#los valores null no se cuentan

# approx_count_distinct

from pyspark.sql.functions import approx_count_distinct
dataframe = spark.read.parquet('./data/vuelos.parquet')

dataframe.printSchema()

dataframe.select(
    countDistinct('AIRLINE'),
    approx_count_distinct('AIRLINE')
).show()

# Funciones min y max

vuelos = spark.read.parquet('./data/vuelos.parquet')

vuelos.printSchema()

from pyspark.sql.functions import min, max, col

vuelos.select(
    min('AIR_TIME').alias('menor_timepo'),
    max('AIR_TIME').alias('mayor_tiempo')
).show()

vuelos.select(
    min('AIRLINE_DELAY'),
    max('AIRLINE_DELAY')
).show()

# Funciones sum, sumDistinct y avg


from pyspark.sql.functions import sum, sumDistinct, avg, count

# sum

vuelos.printSchema()
#suma de la columna distancia
vuelos.select(
    sum('DISTANCE').alias('sum_dis')
).show()

# sumDistinct
#suma solo los valores distintos de la columna, lo que no tiene mucho sentido en este caso
vuelos.select(
    sumDistinct('DISTANCE').alias('sum_dis_dif')
).show()

# avg

vuelos.select(
    avg('AIR_TIME').alias('promedio_aire'),
    (sum('AIR_TIME') / count('AIR_TIME')).alias('prom_manual')
).show()

# Agregación con agrupación

vuelos.printSchema()

from pyspark.sql.functions import desc

(vuelos.groupBy('ORIGIN_AIRPORT')
    .count()
    .orderBy(desc('count'))
).show()

(vuelos.groupBy('ORIGIN_AIRPORT', 'DESTINATION_AIRPORT')
    .count()
    .orderBy(desc('count'))
).show()

# Varias agregaciones por grupo con la función agg()

from pyspark.sql.functions import count, min, max, desc, avg

vuelos.groupBy('ORIGIN_AIRPORT').agg(
    count('AIR_TIME').alias('tiempo_aire'),
    min('AIR_TIME').alias('min'),
    max('AIR_TIME').alias('max')
).orderBy(desc('tiempo_aire')).show()

vuelos.groupBy('MONTH').agg(
    count('ARRIVAL_DELAY').alias('conteo_de_retrasos'),
    avg('DISTANCE').alias('prom_dist')
).orderBy(desc('conteo_de_retrasos')).show()

# Agregación con pivote
estudiantes = spark.read.parquet('./data/estudiantes.parquet')

estudiantes.show()

from pyspark.sql.functions import min, max, avg, col

estudiantes.groupBy('graduacion').pivot('sexo').agg(avg('peso')).show()

estudiantes.groupBy('graduacion').pivot('sexo').agg(avg('peso'), min('peso'), max('peso')).show()

estudiantes.groupBy('graduacion').pivot('sexo', ['M']).agg(avg('peso'), min('peso'), max('peso')).show()

estudiantes.groupBy('graduacion').pivot('sexo', ['F']).agg(avg('peso'), min('peso'), max('peso')).show()

# Inner Join

empleados = spark.read.parquet('./data/empleados.parquet')

departamentos = spark.read.parquet('./data/departamentos.parquet')

empleados.show()

departamentos.show()

from pyspark.sql.functions import col

join_df = empleados.join(departamentos, col('num_dpto') == col('id'))

join_df.show()

join_df = empleados.join(departamentos, col('num_dpto') == col('id'), 'inner')

join_df.show()

join_df = empleados.join(departamentos).where(col('num_dpto') == col('id'))

join_df.show()

# Left Outer Join

empleados.join(departamentos, col('num_dpto') == col('id'), 'leftouter').show()

empleados.join(departamentos, col('num_dpto') == col('id'), 'left_outer').show()

empleados.join(departamentos, col('num_dpto') == col('id'), 'left').show()

# Right Outer Join

empleados.join(departamentos, col('num_dpto') == col('id'), 'rightouter').show()

empleados.join(departamentos, col('num_dpto') == col('id'), 'right_outer').show()

empleados.join(departamentos, col('num_dpto') == col('id'), 'right').show()

# Full Outer Join

empleados.join(departamentos, col('num_dpto') == col('id'), 'outer').show()

# Left Anti Join

#que filas del conjunto de datos de la izquierda no tienen datos coincidentes con el conjunto de la derecha

empleados.join(departamentos, col('num_dpto') == col('id'), 'left_anti').show() #pedro no esta asignado a ningún departamento

departamentos.join(empleados, col('num_dpto') == col('id'), 'left_anti').show()

# Left Semi Join
#similar al anti join pero no muestra los datos de la derecha

empleados.join(departamentos, col('num_dpto') == col('id'), 'left_semi').show() #que trabajadores si estan asignados a algun departamento

# Cross Join

df = empleados.crossJoin(departamentos)

df.show()

df.count()

# Manejo de nombres de columnas duplicados

depa = departamentos.withColumn('num_dpto', col('id'))

depa.printSchema()

empleados.printSchema()

# Devuelve un error
#empleados.join(depa, col('num_dpto') == col('num_dpto'))

# Forma correcta

df_con_duplicados = empleados.join(depa, empleados['num_dpto'] == depa['num_dpto'])

df_con_duplicados.printSchema()

df_con_duplicados.select(empleados['num_dpto']).show()

df2 = empleados.join(depa, 'num_dpto')

df2.printSchema()

empleados.join(depa, ['num_dpto']).printSchema()

# Shuffle Hash Join y Broadcast Hash Join

#Broadcast es para uniones pequeñas, menos memoria
# Shuffle Hash Join para conjuntos más grandes

from pyspark.sql.functions import col, broadcast

empleados.join(broadcast(departamentos), col('num_dpto') == col('id')).show()

empleados.join(broadcast(departamentos), col('num_dpto') == col('id')).explain()