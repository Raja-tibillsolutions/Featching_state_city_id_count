from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csvreader").getOrCreate()

l=["અમાસના","डहाणू","जुन्नर","अमेठी","हवेली तालुका","8855","568qdv","ગણીત","தஞ்சாவூர்","9th","सीकर","7T3H8N",",,","திருச்சி மாவட்டம்","திருப்பூர்","वेल्हा","વિધુત"]

df = spark.read.csv("/home/raja/Desktop/Ekstep/postgresql/data.csv",header=True)


#For user_declared_state is null
df2=df.filter((df['user_declared_state'].isNull()) & (df['user_declared_district'].isNotNull()) ).select('user_declared_state','user_declared_district','device_id','first_access')


#user_declared_district is in random values
df3=df.filter (df['user_declared_district'].isin(l)) .select('user_declared_state','user_declared_district','device_id','first_access')

df2.repartition(1).write.mode("overwrite").format("csv").option("header",True).save("/home/raja/Desktop/Ekstep/user_decleared/output_of_null_state")



