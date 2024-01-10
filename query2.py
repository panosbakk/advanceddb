from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Crime Analysis - Query 2 Df").getOrCreate()

file_path1 = 'hdfs://okeanos-master:54310/user/user/crime_data_2010_2019.csv'
file_path2 = 'hdfs://okeanos-master:54310/user/user/crime_data_2020_present.csv'
df1 = spark.read.csv(file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(file_path2, header=True, inferSchema=True)

df = df1.union(df2)

morning_interval = ((col("TIME OCC") >= 500) & (col("TIME OCC") <= 1159))
afternoon_interval = ((col("TIME OCC") >= 1200) & (col("TIME OCC") <= 1659))
evening_interval = ((col("TIME OCC") >= 1700) & (col("TIME OCC") <= 2059))
night_interval = ((col("TIME OCC") >= 2100) | (col("TIME OCC") <= 359))

crime_data = df.withColumn(
    "day segment",
    when(morning_interval, "Morning")
    .when(afternoon_interval, "Afternoon")
    .when(evening_interval, "Evening")
    .when(night_interval, "Night")
    .otherwise("WRES POU TIS KSEXASATE")
)

result = crime_data.groupBy("day segment").count().orderBy("count", ascending=False)
result.show()

spark.stop()
