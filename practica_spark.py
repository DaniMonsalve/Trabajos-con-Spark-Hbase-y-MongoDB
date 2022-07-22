# -*- coding: utf-8 -*-

from google.colab import drive
drive.mount('/content/drive')

"""## Instalacion de Java

"""

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

!pip install findspark

"""## Instalacion de Spark


"""

from bs4 import BeautifulSoup
import requests

#Obtener las versiones de spark de la pagina web
url = 'https://downloads.apache.org/spark/' 
r = requests.get(url)
html_doc = r.text
soup = BeautifulSoup(html_doc)

# leer la pagina web y obtener las versiones de spark disponibles
link_files = []
for link in soup.find_all('a'):
  link_files.append(link.get('href'))
spark_link = [x for x in link_files if 'spark' in x]  
print(spark_link)

ver_spark = spark_link[1][:-1] # obtener la version y eliminar el caracter '/' del final
print(ver_spark)

import os # libreria de manejo del sistema operativo
#instalar automaticamente la version deseadda de spark
!wget -q https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop2.7.tgz
!tar xf spark-3.2.0-bin-hadoop2.7.tgz
# instalar pyspark
#!pip install -q pyspark

!pip install -q pyspark

"""## Definir variables de entorno"""

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = f"/content/spark-3.2.0-bin-hadoop2.7"

"""# Cargar pyspark en el sistema

##########################33 COMPLETAR ####################################
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
spark = SparkSession.builder.appName(" basket ").master("local[*]").getOrCreate()
spark

"""# Importación

utilizo el entorno temporal que `colab`te genera.

Importa los mismos datos desde tu drive:
"""

import pandas as pd

products = spark.read.options(delimiter=';', inferSchema='True', header=True)\
.csv('/content/drive/MyDrive/IMF/datasets/us_superstores_products.csv')

#transactions_xlsx = pd.read_excel('/content/drive/MyDrive/IMF/datasets/us_superstore_transactions.xlsx') 
#Alguien borro los archivos de la carpeta compartida y ya no funciona este enlace

transactions_xlsx = pd.read_excel('/content/drive/MyDrive/pyspark/us_superstore_transactions.xlsx')

transactions_xlsx.to_csv('transactions',index=False)
transactions=spark.read.options(delimiter=',', inferSchema='True', header=True)\
.csv('transactions')

customers = spark.read.options(delimiter=';', inferSchema='True', header=True)\
.csv('/content/drive/MyDrive/IMF/datasets/us_superstores_customers.csv')

products.limit(5).toPandas()

transactions.limit(5).toPandas()

customers.limit(5).toPandas()

"""# Datasets:
Tenemos 3 datasets:

-Transactions: que contiene la información relativa a órdenes de compra

-Productos

-Clientes

Uno todos los datasets en uno solo, teniendo en cuenta que vamos a analizar la información según orden de compra, producto y cliente. Cada registro (fila) es una entrada de la orden de compra que está asociado a la venta de un product id. La información del cliente se puede repetir en los diferentes registros que componen la orden de compra

"""

df=products.join(transactions,"Product ID","left")
df.show(5)

df=df.join(customers,"Customer ID","left")
df.show(5)

#df1=df.copy()
df1 = df.select('*')

"""

  a. Precio de venta por unidad (teniendo en cuenta el descuento aplicado)

"""

len(df1.columns)

df1.dtypes #Necesito transformar las columnas Quantity, Dicount y Sales a float para que no me de problemas en el paso siguiente.

df1=df1.withColumn("Sales",df1["Sales"].cast("float"))
df1=df1.withColumn("Quantity",df1["Quantity"].cast("float"))
df1=df1.withColumn("Discount",df1["Discount"].cast("float"))
df1.dtypes

import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import array

@udf(FloatType())
def precio_unitario(Sales,Discount,Quantity):
  return (Sales*Discount)/Quantity

df1=df1.withColumn('Precio Unitario',precio_unitario(col('Sales'), col('Discount'),col('Quantity')))


df1.show(10)
len(df1.columns)

"""

b. Beneficio unitario (calculado como beneficio entre cantidad)
"""

df1=df1.withColumn("Beneficio Unitario",df1["Profit"]/df1["Quantity"])
df1.show(10)
len(df1.columns)

"""c. Coste Unitario: calculado como precio de venta unitario - beneficio unitario
"""

df1=df1.withColumn("Coste Unitario",df1["Precio Unitario"]-df1["Beneficio Unitario"])
df1.show(10)
len(df1.columns)

"""2. Identifico la rentabilidad obtenida por cada categoria de producto (profit por categoria de producto).

"""

from pyspark.sql import functions as func
df1=df1.withColumn("Profit",df1["Profit"].cast("float"))
df1.groupBy('Category').mean('Profit').show() # de media la categoría Furniture es la más rentable.
df1.groupBy('Category').sum('Profit').show() #Furniture también es la más rentable sobre el total de categorías

"""3. Identifico los productos a los que se le han aplicado descuento y han obtenido un beneficio negativo en la transacción"""

df1.where((df1['Discount']!=0)&(df1['Profit']<0)).show()
#Muestro los registros que cumplen esas dos condiciones

"""4. Identifico a los clientes a los que se vendieron esos productos y su avg(`Profit')
"""

df2=df1.where((df1['Discount']!=0)&(df1['Profit']<0))
df2.groupBy('Customer ID').mean('Profit').show() #Rentabilidad media según el cliente



#

"""6. Cliente más fiel (medido en número de transacciones) de Florida.

"""
#Me faltaría eliminar los duplicados!

df3=df.filter((df1.State=="Florida"))

df4=df3.groupBy('Customer Name').count()

df4.orderBy(col('count').desc()).show(10) # La clienta con más transacciones es Emily Phan.

