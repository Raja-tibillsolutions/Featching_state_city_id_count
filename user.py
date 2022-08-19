from pyspark.sql import SparkSession
from pyspark.sql.functions import json_tuple

spark=SparkSession.builder.appName("read").getOrCreate()

df=spark.read.csv("/home/raja/Desktop/Ekstep/cassandra/user/diksha_user.csv",header=True,sep='|')
df.printSchema()

df_filter = df.filter(df['profileusertype'].isNull()).select('id','userid','usertype','createddate')

df_filter.repartition(1).write.mode('overwrite').format('csv').option('header',True).option('sep','|').save("/home/raja/Desktop/Ekstep/cassandra/user/output_usertypenull")