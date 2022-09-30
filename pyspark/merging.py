from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("merging").getOrCreate()

df=spark.read.csv("/home/raja/Downloads/13th Sept 2022 UP Textbook - TB Plays 13th Sep 2022.csv",header=True)
df1=spark.read.csv("/home/raja/Downloads/13th Sept 2022 UP Textbook - TB Devices 13th Sep 2022.csv",header=True)

df_join =df1.join(df,['collection_gradelevel','collection_name','collection_channel_slug','collection_board','collection_subject','object_rollup_l1','collection_medium'],how="inner")
df_join.repartition(1).write.mode("overwrite").format("csv").option('header',True).save("/home/raja/Desktop/Ekstep/postgresql/up")