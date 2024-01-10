from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("Crime Analysis - Query 2 Rdd").getOrCreate()

file_path1 = 'hdfs://okeanos-master:54310/user/user/crime_data_2010_2019.csv'
file_path2 = 'hdfs://okeanos-master:54310/user/user/crime_data_2020_present.csv'
df1 = spark.read.csv(file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(file_path2, header=True, inferSchema=True)

df = df1.union(df2)

# Convert DataFrame to RDD
rdd = df.rdd

def map_to_day_segment(row):
    time_occ = int(row["TIME OCC"])
    
    if 500 <= time_occ <= 1159:
        return "Morning"
    elif 1200 <= time_occ <= 1659:
        return "Afternoon"
    elif 1700 <= time_occ <= 2059:
        return "Evening"
    elif 2100 <= time_occ or time_occ <= 359:
        return "Night"
    else:
        return "WRES POU TIS KSEXASATE"

# Apply the map function to each row
mapped_rdd = rdd.map(map_to_day_segment)

# Count occurrences of each day segment
counted_rdd = mapped_rdd.countByValue()

# Convert the result to a list of tuples and create an RDD
result_rdd = spark.sparkContext.parallelize(list(counted_rdd.items()))

# Convert the result RDD to a DataFrame
result_df = result_rdd.toDF(["day segment", "count"])

# Order the result DataFrame
result_df = result_df.orderBy("count", ascending=False)

# Show the result
result_df.show()

spark.stop()
