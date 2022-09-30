from pyspark.sql import SparkSession
import pandas
import sys

user_declared_state = sys.argv[1]
print(user_declared_state,'state:::::::::::::')


df_mapping = pandas.read_csv('/home/raja/Downloads/Recent UP TextBook 23rd Aug2022 - state_district.csv')
df_mapping = df_mapping.loc[df_mapping['state'] == user_declared_state]
city_list = list(df_mapping['district'])

spark = SparkSession.builder.appName('getnishta').getOrCreate()


df1= spark.read.csv("/home/raja/Downloads/13th Sept 2022 UP Textbook - QR Scans 13th Sep 2022.csv",header=True)
#df2 = spark.read.csv("/home/raja/Downloads/Recent UP TextBook 23rd Aug2022 - District wise Plays.csv",header=True)
#df1.printSchema()
#df2.printSchema()
#df3 = df1.join(df2,['user_loc_block','state_slug','user_loc_cluster','user_school_name','derived_loc_district'],how='inner')
df4=df1.filter(df1["derived_loc_district"].isin(city_list)).select('derived_loc_district','state_slug','SUM(total_count)')

#df3=df1.filter(df2['user_loc_block','state_slug','user_loc_cluster','user_school_name',' derived_loc_district'] == df3['user_loc_block','state_slug','user_loc_cluster','user_school_name',' derived_loc_district']).select('user_loc_block','state_slug','user_loc_cluster','user_school_name',' derived_loc_district','total_unique_devices','SUM(total_count)')
df4.repartition(1).write.mode('overwrite').format('csv').option('header',True).save("/home/raja/Desktop/Ekstep/up/output2")
