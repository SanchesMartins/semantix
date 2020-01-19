# 1. Número de hosts únicos.
# 2. O total de erros 404.
# 3. Os 5 URLs que mais causaram erro 404.
# 4. Quantidade de erros 404 por dia.
# 5. O total de bytes retornados.

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext()
sqlContext = SQLContext(sc)

path = r".\data"
schema = StructType([
    StructField("host", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("request", StringType(), True),
    StructField("code", StringType(), True),
    StructField("bytes", StringType(), True)
])


def normalize(row):
    global schema
    row = row.strip().replace('\t', '')
    row = row[: row.find("\"") + 1] + row[row.find("\"") + 1: row.rfind("\"")].replace(" ", "-") + row[row.rfind("\""):]
    row = row[: row.find("[")] + row[row.find("[") + 1: row.find("]")].replace(" ", "") + row[row.rfind("]") + 1:]
    row = row.replace(" - - ", "\t").replace(" ", "\t")
    row = row.split('\t')
    if len(row) != len(schema):
        while(len(row) < len(schema)):
            row.append(None)
    return row


data = sc.textFile(path)
data = data.map(normalize)
data = data.toDF(schema).withColumn("datetime", to_timestamp("datetime", 'dd/MMM/yyyy:HH:mm:ssZ'))
data = data.cache()
data.show(30)

# contagem de hosts unicos
data.select('host').distinct().count()

# contagem de 404
data.where(data.code == '404').count()

# top 5 http: 404
data.registerTempTable('tmp')
sqlContext.sql('select * from (select host, count(*) as contagem from tmp where code = '
               '\'404\' group by host) order by contagem desc limit 5').show()

# quantidade de 404 por dia
sqlContext.sql('select from_unixtime(unix_timestamp(datetime), \'yyyy-MM-dd\') as date, '
               'count(code) as 404_errors from tmp where code = \'404\' ' +
               'group by from_unixtime(unix_timestamp(datetime), \'yyyy-MM-dd\')').orderBy('date').show()


# quantidade de bytes
data.selectExpr('cast(bytes as bigint)').groupBy().sum().show()
