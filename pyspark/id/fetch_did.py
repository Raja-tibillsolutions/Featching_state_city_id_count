from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark =SparkSession.builder.appName('get_did').getOrCreate()
df =spark.read.csv('/home/raja/Desktop/Ekstep/device_dump/device_profil-8jul.csv',header=True)
df =df.groupBy('state','city').agg(F.count('device_id').alias('count_of_did'))
df.repartition(1).write.mode("overwrite").format("csv").option("header",True).save("home/raja/device_id_output")

df.printSchema()