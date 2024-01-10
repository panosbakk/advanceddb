from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DateType, IntegerType, DoubleType, StringType

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Read the CSV file into a Spark DataFrame
file_path1 = 'hdfs://okeanos-master:54310/user/user/crime_data_2010_2019.csv'
file_path2 = 'hdfs://okeanos-master:54310/user/user/crime_data_2020_present.csv'
df1 = spark.read.csv(file_path1, header=True, inferSchema=True)
df2 = spark.read.csv(file_path2, header=True, inferSchema=True)

df = df1.union(df2)
output_path = 'hdfs://okeanos-master:54310/user/user/crime_data.csv'
df.write.csv(output_path, header=True, mode='overwrite')

# Adjust data types with the correct date format
date_format = 'MM/dd/yyyy hh:mm:ss a'
df = df.withColumn("Date Rptd", to_date(col("Date Rptd"), date_format).cast(DateType()))
df = df.withColumn("DATE OCC", to_date(col("DATE OCC"), date_format).cast(DateType()))
df = df.withColumn("Vict Age", df["Vict Age"].cast(IntegerType()))
df = df.withColumn("LAT", df["LAT"].cast(DoubleType()))
df = df.withColumn("LON", df["LON"].cast(DoubleType()))

# Show the total number of rows
total_rows = df.count()
print(f"Total number of rows: {total_rows}")

# Show the data types of each column
column_types = [(col, str(df.schema[col].dataType)) for col in df.columns]
print("\nData types of each column:")
for col, col_type in column_types:
    print(f"{col}: {col_type}")

# Stop the Spark session
spark.stop()

