from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, radians, sin, cos, sqrt, atan2, row_number
from pyspark.sql.types import DateType, IntegerType, DoubleType
from pyspark.sql.window import Window

# Define the Haversine formula for distance calculation
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = radians(lat1), radians(lon1), radians(lat2), radians(lon2)
    dlat, dlon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

spark = SparkSession.builder.appName("Crime Analysis - Query 4").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

file_path1 = "hdfs://okeanos-master:54310/user/user/crime_data_2010_2019.csv"
file_path2 = "hdfs://okeanos-master:54310/user/user/crime_data_2020_present.csv"
file_path3 = "hdfs://okeanos-master:54310/user/user/police.csv"

df1 = spark.read.csv(file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(file_path2, header=True, inferSchema=True)
crime_data = df1.union(df2)

police_stations = spark.read.csv(file_path3, header=True, inferSchema=True)

date_format = 'MM/dd/yyyy hh:mm:ss a'
crime_data = (
    crime_data
    .withColumn("Date Rptd", to_date(col("Date Rptd"), date_format).cast(DateType()))
    .withColumn("DATE OCC", to_date(col("DATE OCC"), date_format).cast(DateType()))
    .withColumn("Vict Age", crime_data["Vict Age"].cast(IntegerType()))
    .withColumn("LAT", crime_data["LAT"].cast(DoubleType()))
    .withColumn("LON", crime_data["LON"].cast(DoubleType()))
    .filter((col("LAT") != 0) & (col("LON") != 0))
)

query4_a = (
    crime_data
    .join(police_stations, crime_data["AREA "] == police_stations["PREC"])
    .withColumn("distance", haversine(col("LAT"), col("LON"), col("Y"), col("X")))
)

result_1 = (
    query4_a
    .withColumn("year", year("DATE OCC"))
    .groupBy("year")
    .agg({"distance": "avg", "DATE OCC": "count"})
    .withColumnRenamed("avg(distance)", "average_distance")
    .withColumnRenamed("count(DATE OCC)", "#")
    .orderBy("year")
)

result_2 = (
    query4_a
    .groupBy("DIVISION")
    .agg({"distance": "avg", "DATE OCC": "count"})
    .withColumnRenamed("avg(distance)", "average_distance")
    .withColumnRenamed("count(DATE OCC)", "#")
    .orderBy("#", ascending=False)
)

query4_b = (
    crime_data
    .crossJoin(police_stations)
    .withColumn("distance", haversine(col("LAT"), col("LON"), col("Y"), col("X")))
    .withColumn("rank", row_number().over(Window.partitionBy("DR_NO").orderBy("distance")))
    .filter(col("rank") == 1)
    .drop("rank")
)

result_3 = (
    query4_b
    .withColumn("year", year("DATE OCC"))
    .groupBy("year")
    .agg({"distance": "avg", "DATE OCC": "count"})
    .withColumnRenamed("avg(distance)", "average_closest_distance")
    .withColumnRenamed("count(DATE OCC)", "#")
    .orderBy("year")
)

result_4 = (
    query4_b
    .groupBy("DIVISION")
    .agg({"distance": "avg", "DATE OCC": "count"})
    .withColumnRenamed("avg(distance)", "average_closest_distance")
    .withColumnRenamed("count(DATE OCC)", "#")
    .orderBy("#", ascending=False)
)

result_1.show()
result_2.show(n=result_2.count(), truncate=False)
result_3.show()
result_4.show(n=result_4.count(), truncate=False)

spark.stop()