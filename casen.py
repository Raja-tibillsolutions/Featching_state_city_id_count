from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import ast
spark = SparkSession.builder.appName("azure_data_agg").getOrCreate()


def data(a):
 if a is not None:
      b='"'+a+'"'
      row = json.loads(b)
      out= json.loads(row)
      return out['type']


get_data=udf(data, StringType())




path = '/home/raja/Desktop/Ekstep/cassandra/user/output_usertypenull/output.csv'
df = spark.read.csv(path, header=True, sep='|')
df=df.withColumn('user_type',get_data(col("profileusertype")))

df = df.select('id', 'userid', 'user_type', 'createddate')
df.repartition(1).write.csv(path="/home/raja/Desktop/Ekstep/cassandra/user/output_usertypenull/output", mode="overwrite", sep="|", header="true")