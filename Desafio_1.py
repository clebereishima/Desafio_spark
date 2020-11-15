import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
import io
import zipfile
import os


conf = (
    SparkConf()
    .setAppName("Spark SFTP Test")
    .set("spark.hadoop.fs.sftp.impl", "org.apache.hadoop.fs.sftp.SFTPFileSystem")
)
sc = SparkContext(conf=conf).getOrCreate()

#Case 1
#Criar função que consome dados de um ftp em Spark. O Download do arquivo deve ser um .zip e descompactar em data frame para salvar em ambiente.

schema = StructType([
    StructField("transacao", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("data_atualizacao", StringType(), True)
])

def unzip_files(row):
    content = row
    zfile = zipfile.ZipFile(io.BytesIO(content), "r")
    files = [i for i in zfile.namelist()]
    return zfile.open(files[0]).read().decode("utf-8", errors='ignore')

source_file_path = 'data/test.zip'

df_unzip = sc \
    .binaryFiles(f"ftp://{os.environ.get('FTP_USER')}:{os.environ.get('FTP_PASS')}@{os.environ.get('FTP_HOST')}/{source_file_path}")\
    .map(unzip_files)\
    .map(lambda row: row.strip())\
    .flatMap(lambda row: str(row).split('\n'))\
    .map(lambda row: str(row).split(';'))\
    .toDF(schema=schema)\
    .show()


