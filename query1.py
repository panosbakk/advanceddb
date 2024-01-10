from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, rank, sum
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, IntegerType, DoubleType

# Create a Spark session
spark = SparkSession.builder.appName("Crime Analysis - Query 1 Df").getOrCreate()

# Read the CSV files into Spark DataFrames
file_path1 = 'hdfs://okeanos-master:54310/user/user/crime_data_2010_2019.csv'
file_path2 = 'hdfs://okeanos-master:54310/user/user/crime_data_2020_present.csv'
df1 = spark.read.csv(file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(file_path2, header=True, inferSchema=True)

# Union the two DataFrames
df = df1.union(df2)

# Adjust data types with the correct date format
date_format = 'MM/dd/yyyy hh:mm:ss a'
df = df.withColumn("Date Rptd", to_date(col("Date Rptd"), date_format).cast(DateType()))
df = df.withColumn("DATE OCC", to_date(col("DATE OCC"), date_format).cast(DateType()))
df = df.withColumn("Vict Age", df["Vict Age"].cast(IntegerType()))
df = df.withColumn("LAT", df["LAT"].cast(DoubleType()))
df = df.withColumn("LON", df["LON"].cast(DoubleType()))

# Extract year and month from 'DATE OCC'
df = df.withColumn("year", year("DATE OCC"))
df = df.withColumn("month", month("DATE OCC"))

# Calculate the total number of crimes for each year and month
agg_df = df.groupBy("year", "month").agg(
    sum((col("Crm Cd 1").isNotNull()).cast(IntegerType()) +
        (col("Crm Cd 2").isNotNull()).cast(IntegerType()) +
        (col("Crm Cd 3").isNotNull()).cast(IntegerType()) +
        (col("Crm Cd 4").isNotNull()).cast(IntegerType())
    ).alias("crime_total")
)

# Create a WindowSpec for ranking
windowSpec = Window.partitionBy("year").orderBy(col("crime_total").desc())

# Add a rank column based on crime_total
df_with_rank = agg_df.withColumn("#", rank().over(windowSpec))

# Filter the top 3 months for each year
result = df_with_rank.filter(col("#") <= 3).orderBy("year", "#")

# Show the results
result.show(n=result.count(), truncate=False)

# Stop the Spark session
spark.stop()
