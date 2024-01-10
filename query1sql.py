from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month, dense_rank
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("Crime Analysis - Query 1 Sql").getOrCreate()

# Load datasets into PySpark DataFrames
crime_data_2010_to_2019 = spark.read.csv("crime_data_2010_2019.csv", header=True, inferSchema=True)
crime_data_2020_to_present = spark.read.csv("crime_data_2020_present.csv", header=True, inferSchema=True)

# Union the two DataFrames
crime_data = crime_data_2010_to_2019.union(crime_data_2020_to_present)

# Convert the 'DATE OCC' column to a timestamp type
crime_data = crime_data.withColumn('timestamp', to_timestamp('DATE OCC', 'MM/dd/yyyy hh:mm:ss a'))

# Register the DataFrame as a temporary SQL table
crime_data.createOrReplaceTempView("crime_data")

# SQL query to find the top 3 months with the highest crime count for each year
query = """
    SELECT year, month, crime_total, `#`
    FROM (
        SELECT
            year(timestamp) as year,
            month(timestamp) as month,
            SUM(CASE WHEN `Crm Cd 1` IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN `Crm Cd 2` IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN `Crm Cd 3` IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN `Crm Cd 4` IS NOT NULL THEN 1 ELSE 0 END) as crime_total,
            DENSE_RANK() OVER (PARTITION BY year(timestamp) ORDER BY COUNT(*) DESC) AS `#`
        FROM crime_data
        GROUP BY year(timestamp), month(timestamp)
    ) temp
    WHERE `#` <= 3
"""
# Execute the query
result = spark.sql(query)

# Show the result
result.show(n=result.count(), truncate=False)
spark.stop()
