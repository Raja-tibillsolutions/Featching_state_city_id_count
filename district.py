from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("jsontocsv").getOrCreate()

l=[ 'Belagavi',
    'Bagalkot',
    'Vijayapura',
    'Kalaburagi',
    'Bidar',
    'Raichur',
    'Koppal',
    'Gadag',
    'Dharwad',
    'Uttara Kannada',
    'Haveri',
    'Ballari',
    'Chitradurga',
    'Davanagere',
    'Shivamogga',
    'Udupi',
    'Chikkamangaluru',
    'Tumakuru',
    'Kolar',
    'Bengaluru U South',
    'Bengaluru Rural',
    'Mandya',
    'Hassan',
    'Dakshina Kannada',
    'Kodagu',
    'Mysuru',
    'Chamarajanagara',
    'Bengaluru U North',
    'Chikkaballapura',
    'Belagavi Chikkodi',
    'Tumakuru Madhugiri','Ramanagara','Yadagiri','Uttara Kannada Sirsi']

df = spark.read.csv("/home/raja/Desktop/Ekstep/device_dump/device_profil-8jul.csv",header=True).select("state","city","device_id")
df1=df.filter((df.state != "Karnataka") & df["city"].isin(l))
df1.repartition(1).write.mode("overwrite").format("csv").option("header",True).save("/home/raja/Desktop/Ekstep/device_dump/karnataka_output")
