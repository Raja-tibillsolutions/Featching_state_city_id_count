from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark=SparkSession.builder.appName('loweandupper').getOrCreate()

#reading csv
df=spark.read.csv("/home/raja/Desktop/Ekstep/cassandra/user/diksha_user.csv",header=True,sep='|')

df.filter(df.usertype == 'student').agg(f.count('userid')).show()
