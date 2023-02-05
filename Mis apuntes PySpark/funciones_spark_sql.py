# -*- coding: utf-8 -*-
"""Funciones_Spark_SQL.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/19DE6uCcn9kQcXykhrN9ePJ8gyK7S5K71
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

#Inicio una sesion de spark

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

data = spark.read.parquet('./data/convertir')

data.printSchema()

data.show(truncate=False)

from pyspark.sql.functions import col, to_date, to_timestamp

data1 = data.select(
    to_date(col('date')).alias('date1'),
    to_timestamp(col('timestamp')).alias('ts1'),
    to_date(col('date_str'), 'dd-MM-yyyy').alias('date2'),
    to_timestamp(col('ts_str'), 'dd-MM-yyyy mm:ss').alias('ts2')

)

data1.show(truncate=False)

data1.printSchema()

#cambiar el formato de fecha
from pyspark.sql.functions import date_format

data1.select(
    date_format(col('date1'), 'dd-MM-yyyy')
).show()

#Cambio de df

df = spark.read.parquet('./data/calculo.parquet')

df.show()

#diferencia de fechas

from pyspark.sql.functions import datediff, months_between, last_day

df.select(
    col('nombre'),
    datediff(col('fecha_salida'), col('fecha_ingreso')).alias('dias'),
    months_between(col('fecha_salida'), col('fecha_ingreso')).alias('meses'),
    last_day(col('fecha_salida')).alias('ultimo_dia_mes')
).show()

from pyspark.sql.functions import date_add, date_sub

df.select(
    col('nombre'),
    col('fecha_ingreso'),
    date_add(col('fecha_ingreso'), 14).alias('mas_14_dias'),
    date_sub(col('fecha_ingreso'), 1).alias('menos_1_dia')
).show()

from pyspark.sql.functions import year, month, dayofmonth, dayofyear, hour, minute, second

df.select(
    col('baja_sistema'),
    year(col('baja_sistema')),
    month(col('baja_sistema')),
    dayofmonth(col('baja_sistema')),
    dayofyear(col('baja_sistema')),
    hour(col('baja_sistema')),
    minute(col('baja_sistema')),
    second(col('baja_sistema'))
).show()

# Funciones para trabajo con strings

data = spark.read.parquet('./data/data.parquet')

data.show() #espacios a la izquierda y a la derecha

from pyspark.sql.functions import ltrim, rtrim, trim #para eliminar espacios

data.select(
    ltrim('nombre').alias('ltrim'),#eliminar espacio a la izquierda
    rtrim('nombre').alias('rtrim'),#eliminar espacio a la derecha
    trim('nombre').alias('trim')#eliminar espacio a ambos lados
).show()

from pyspark.sql.functions import col, lpad, rpad

data.select(
    trim(col('nombre')).alias('trim')
).select(
    lpad(col('trim'), 8, '-').alias('lpad'),
    rpad(col('trim'), 8, '=').alias('rpad')
).show()

df1 = spark.createDataFrame([('Spark', 'es', 'maravilloso')], ['sujeto', 'verbo', 'adjetivo'])

df1.show()

from pyspark.sql.functions import concat_ws, lower, upper, initcap, reverse

df1.select(
    concat_ws(' ', col('sujeto'), col('verbo'), col('adjetivo')).alias('frase')
).select(
    col('frase'),
    lower(col('frase')).alias('minuscula'),
    upper(col('frase')).alias('mayuscula'),
    initcap(col('frase')).alias('initcap'),
    reverse(col('frase')).alias('reversa')
).show()

from pyspark.sql.functions import regexp_replace

df2 = spark.createDataFrame([(' voy a casa por mis llaves',)], ['frase'])

df2.show(truncate=False)


#remplazar palabras
df2.select(
    regexp_replace(col('frase'), 'voy|por', 'ir').alias('nueva_frase')
).show(truncate=False)

# Funciones para trabajo con colecciones

data = spark.read.parquet('./data/parquet/')

data.show(truncate=False)

data.printSchema()

from pyspark.sql.functions import col, size, sort_array, array_contains

# función contains

data.select(
    size(col('tareas')).alias('tamaño'),
    sort_array(col('tareas')).alias('arreglo_ordenado'),
    array_contains(col('tareas'), 'buscar agua').alias('buscar_agua')
).show(truncate=False)

from pyspark.sql.functions import explode

#Asociar tareas al día de la semana

data.select(
    col('dia'),
    explode(col('tareas')).alias('tareas')
).show()

# Formato JSON

json_df_str = spark.read.parquet('./data/json')

json_df_str.show(truncate=False)

json_df_str.printSchema()

from pyspark.sql.types import StructType, StructField, StringType, ArrayType

#necesitamos crear un esquema json para poder leer el json

schema_json = StructType(
    [
     StructField('dia', StringType(), True),
     StructField('tareas', ArrayType(StringType()), True)
    ]
)

from pyspark.sql.functions import from_json, to_json

json_df = json_df_str.select(
    from_json(col('tareas_str'), schema_json).alias('por_hacer')
)

json_df.printSchema()

json_df.select(
    col('por_hacer').getItem('dia'),
    col('por_hacer').getItem('tareas'),
    col('por_hacer').getItem('tareas').getItem(0).alias('primer_tarea') #obtener la primera tarea en la posición 0
).show(truncate=False)

#Inversamente: convertir un string en un json
json_df.select(
    to_json(col('por_hacer'))
).show(truncate=False)

# Funciones when, coalesce y lit

data = spark.read.parquet('./data/data2')

data.show()

from pyspark.sql.functions import col, when, lit, coalesce

data.select(
    col('nombre'),
    when(col('pago') == 1, 'pagado').when(col('pago') == 2, 'sin pagar').otherwise('sin iniciar').alias('pago')
).show()

data.select(
    coalesce(col('nombre'), lit('sin nombre')).alias('nombre')
).show()

# Funciones definidas por el usuario UDF

def cubo(n):
    return n * n * n

from pyspark.sql.types import LongType

spark.udf.register('cubo', cubo, LongType())

spark.range(1,10).createOrReplaceTempView('df_temp')

spark.sql("SELECT id, cubo(id) AS cubo FROM df_temp").show()

def bienvenida(nombre):
    return ('Hola {}'.format(nombre))

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

bienvenida_udf = udf(lambda x: bienvenida(x), StringType())

df_nombre = spark.createDataFrame([('Jose',), ('Julia',)], ['nombre'])

df_nombre.show()

from pyspark.sql.functions import col

df_nombre.select(
    col('nombre'),
    bienvenida_udf(col('nombre')).alias('bie_nombre')
).show()

@udf(returnType=StringType())
def mayuscula(s):
    return s.upper()

df_nombre.select(
    col('nombre'),
    mayuscula(col('nombre')).alias('may_nombre')
).show()

# Las UDF de pandas tienen un mejor rendimiento
import pandas as pd

from pyspark.sql.functions import pandas_udf

def cubo_pandas(a: pd.Series) -> pd.Series:
    return a * a * a

cubo_udf = pandas_udf(cubo_pandas, returnType=LongType())

x = pd.Series([1, 2, 3])

print(cubo_pandas(x))

df = spark.range(5)

df.select(
    col('id'),
    cubo_udf(col('id')).alias('cubo_pandas')
).show()

# Funciones de ventana

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data/data3')

df.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number, rank, dense_rank, col

windowSpec = Window.partitionBy('departamento').orderBy(desc('evaluacion')) #particion por departamento

# row_number
#parada dar el número de filas secuencial

df.withColumn('row_number', row_number().over(windowSpec)).filter(col('row_number').isin(1,2)).show() #limitando a 2 filas, si no limitara me saldria hasta el número de usuarios en cada departamento. Estoy mostrando los dos mejores de cada departamento

# rank
# problema con los empates?? rangos que desaparecen
df.withColumn('rank', rank().over(windowSpec)).show()

# dense_rank
# soluciona los empates
df.withColumn('dense_rank', dense_rank().over(windowSpec)).show()

# Agregaciones con especificaciones de ventana
#no es necesario el orderby
windowSpecAgg = Window.partitionBy('departamento')

from pyspark.sql.functions import min, max, avg

(df.withColumn('min', min(col('evaluacion')).over(windowSpecAgg))
.withColumn('max', max(col('evaluacion')).over(windowSpecAgg))
.withColumn('avg', avg(col('evaluacion')).over(windowSpecAgg))
.withColumn('row_number', row_number().over(windowSpec))
 ).show()

# Catalyst Optimizer

#data = spark.read.parquet('./data/data3')
data = spark.read.parquet('./data/vuelos.parquet')

data.printSchema()

data.show()

from pyspark.sql.functions import col

nuevo_df = (data.filter(col('MONTH').isin(6,7,8))
            .withColumn('dis_tiempo_aire', col('DISTANCE') / col('AIR_TIME'))
).select(
    col('AIRLINE'),
    col('dis_tiempo_aire')
).where(col('AIRLINE').isin('AA', 'DL', 'AS'))

nuevo_df.explain(True)