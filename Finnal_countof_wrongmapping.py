from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("jsontocsv").getOrCreate()

df = spark.read.csv("/home/raja/Desktop/Ekstep/user_decleared_district/output_for_null wrongmapping/*",header=True)

df.repartition(1).write.mode("overwrite").format("csv").option("header", True).save("/home/raja/Desktop/Ekstep/user_decleared_district/overallnull")
#df_agg = df.groupBy('state', 'city').agg(F.count('device_id').alias('count_of_did'))
#df_agg.repartition(1).write.mode("overwrite").format("csv").option("header", True).save("/home/raja/Desktop/Ekstep/user_decleared_district/nullcount_ofdid")

