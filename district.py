from pyspark.sql import SparkSession
import pandas
import sys

user_declared_state = sys.argv[1]
print(user_declared_state,'state:::::::::::::')

spark=SparkSession.builder.appName("jsontocsv").getOrCreate()

df_mapping = pandas.read_csv('/home/raja/Desktop/Ekstep/State_city_mapping/State,District -Unmapping Report - mapping.csv')
df_mapping = df_mapping.loc[df_mapping['State'] == user_declared_state]
city_list = list(df_mapping['city'])



df = spark.read.csv("/home/raja/Desktop/Ekstep/user_decleared_district/output_of_null_distric/*.csv",header=True).select("state","city","device_id","first_access")
#df2=df.filter(df['user_declared_state'].isNull)
df1 = df.filter((df.state != user_declared_state) & df["city"].isin(city_list) )
#df2 = df.filter((df.user_declared_district != city_list))
#df_merage= df2.unionByName(df1)
df1.repartition(1).write.mode("append").format("csv").option("header",True).save("/home/raja/Desktop/Ekstep/user_decleared_district/output_for_null wrongmapping")
