import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession \
    .builder \
    .appName("Desafio 2 Roche") \
    .getOrCreate()

#Case 2  

#Criar um data frame com transações por municípios e data de atualização.
#Considerando o data frame agrupar por município e considerando a data de atualização definir a ordem da transação.

#Exemplo de schema:
#Transação, Município, Estado e Data de atualização

#O data frame final precisa conter um campo ordenando as transações por município
#Transação, Município, Estado, data de Atualização e Ordem da Transação

#Obs. Alimentar o data frame com mais de uma transação por município e mais de um município, mas não precisa ser um grande volume.
#O objetivo é avaliar a lógica da solução e a organização do código em PySpark.


#Schema do Data frame
schema = StructType([
    StructField("transacao", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("data_atualizacao", StringType(), True)
])

#Leitura do arquivo 
df_transacao = spark.read.option("delimiter", ";").schema(schema).csv("F:\\Utilidades\\teste.txt")

df_transacao.show()
+---------+----------+--------------+-------------------+
|transacao| municipio|        estado|   data_atualizacao|
+---------+----------+--------------+-------------------+
|    2,000|    Barras|         Piaui|01-09-1995 09:01:17|
|    2,500|Adamantina|     Sao Paulo|01-09-1995 09:10:37|
|    2,500|Adamantina|     Sao Paulo|01-09-1998 09:11:17|
|    2,100|Adamantina|     Sao Paulo|01-09-1999 09:09:17|
|    2,000|    Barras|         Piaui|01-09-1995 09:09:17|
|    3,000|  Garopaba|Santa Catarina|01-09-1997 09:09:17|
+---------+----------+--------------+-------------------+

#Criando a ordem da transação ordenando pelo campo data atualização e posteriormente ordenando por Municipio
df_final = df_transacao.withColumn("ordem_da_transação", row_number().over(Window.partitionBy("municipio").orderBy("data_atualizacao")))
df_final.orderBy("municipio", "ordem_da_transação").show()

+---------+----------+--------------+-------------------+------------------+
|transacao| municipio|        estado|   data_atualizacao|ordem_da_transação|
+---------+----------+--------------+-------------------+------------------+
|    2,500|Adamantina|     Sao Paulo|01-09-1995 09:10:37|                 1|
|    2,500|Adamantina|     Sao Paulo|01-09-1998 09:11:17|                 2|
|    2,100|Adamantina|     Sao Paulo|01-09-1999 09:09:17|                 3|
|    2,000|    Barras|         Piaui|01-09-1995 09:01:17|                 1|
|    2,000|    Barras|         Piaui|01-09-1995 09:09:17|                 2|
|    3,000|  Garopaba|Santa Catarina|01-09-1997 09:09:17|                 1|
+---------+----------+--------------+-------------------+------------------+