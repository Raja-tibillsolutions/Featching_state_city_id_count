from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("date").getOrCreate()

df = spark.read.csv("/home/raja/Desktop/Ekstep/cassandra/user/output_usertypenull/outpu_of_profile_usertype.csv",header=True,sep='|')
df1=df.withColumn('createddate',to_date(col('createddate')))
df_filter =df.filter(col("createddate").between('2022-08-01','2022-09-01')).agg(count('userid')).show()

#df_filter.groupBy('createddate').agg(countDistinct('userid')).show()