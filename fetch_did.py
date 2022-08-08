from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark =SparkSession.builder.appName('get_did').getOrCreate()
df =spark.read.csv('/home/raja/Desktop/Ekstep/user_decleared_district/output_of_null_distric/*.csv',header=True)

df1 =df.groupBy('state','city').agg(F.count('device_id').alias('count_of_did'))
df1.repartition(1).write.mode("overwrite").format("csv").option("header",True).save("/home/raja/Desktop/Ekstep/user_decleared_district/overall")

#df.printSchema()