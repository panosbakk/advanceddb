from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, rank, sum, split, expr
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, IntegerType, DoubleType

# Create a Spark session
spark = SparkSession.builder.appName("Crime Analysis - Query 3").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Load crime data
crime_data_path = "hdfs://okeanos-master:54310/user/user/crime_data_2010_2019.csv"
crime_df = spark.read.csv(crime_data_path, header=True, inferSchema=True)

# Adjust data types with the correct date format
date_format = 'MM/dd/yyyy hh:mm:ss a'
crime_df = crime_df.withColumn("Date Rptd", to_date(col("Date Rptd"), date_format).cast(DateType())) 
crime_df = crime_df.withColumn("DATE OCC", to_date(col("DATE OCC"), date_format).cast(DateType()))
crime_df = crime_df.withColumn("Vict Age", crime_df["Vict Age"].cast(IntegerType())) 
crime_df = crime_df.withColumn("LAT", crime_df["LAT"].cast(DoubleType()))
crime_df = crime_df.withColumn("LON", crime_df["LON"].cast(DoubleType()))

# Load police stations data
police_stations_path = "hdfs://okeanos-master:54310/user/user/revgecoding.csv"  # Replace with the actual path
police_stations_df = spark.read.csv(police_stations_path, header=True, inferSchema=True)
police_stations_df = police_stations_df.withColumn("ZIPcode", split(col("ZIPcode"), "-").getItem(0))

# Load income data
income_data_path = "hdfs://okeanos-master:54310/user/user/LA_income_2015.csv"  # Replace with the actual path
income_df = spark.read.csv(income_data_path, header=True, inferSchema=True)

# Create an alias for the ZIPcode column in police_stations_df
police_stations_df = police_stations_df.withColumnRenamed("ZIPcode", "Zip Code")

# Define the join conditions
join_condition_crime = (crime_df["LAT"] == police_stations_df["LAT"]) & (crime_df["LON"] == police_stations_df["LON"])
join_condition_income = police_stations_df["Zip Code"] == income_df["Zip Code"]

# Perform inner joins and select the columns you need
joined_df = (
    crime_df
    .join(police_stations_df, (crime_df["LAT"] == police_stations_df["LAT"]) & (crime_df["LON"] == police_stations_df["LON"]), "inner")
    .join(income_df, police_stations_df["Zip Code"] == income_df["Zip Code"], "inner")
    .select(
        crime_df["*"],  # Select all columns from the crime_df
        police_stations_df["Zip Code"].alias("Zip Code"),
        income_df["Estimated Median Income"].alias("Median Household Income 2015")
    )
)

# Filter data for the year 2015
filtered_df = joined_df.filter((year(col("DATE OCC")) == 2015) & (col("Vict Descent").isNotNull()))

# Group by Zip Code, Descent, and count the number of victims
result_df = filtered_df.groupBy("Zip Code", "Vict Descent").agg(sum("Vict Age").alias("Victim Count"))

# Rank the data based on the count of victims within each Zip Code, in descending order
window_spec = Window.partitionBy("Zip Code").orderBy(col("Victim Count").desc())
ranked_df = result_df.withColumn("#", rank().over(window_spec))

# Mapping dictionary for Vict Descent and Descent Code
descent_mapping = {
    'A': 'Other Asian', 'B': 'Black', 'C': 'Chinese', 'D': 'Cambodian',
    'F': 'Filipino', 'G': 'Guamanian', 'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native', 'J': 'Japanese', 'K': 'Korean',
    'L': 'Laotian', 'O': 'Other', 'P': 'Pacific Islander', 'S': 'Samoan',
    'U': 'Hawaiian', 'V': 'Vietnamese', 'W': 'White', 'X': 'Unknown',
    'Z': 'Asian Indian'
}

# Create a SQL expression to apply the mapping
sql_expr = "CASE WHEN `Vict Descent` IN ({}) THEN {} ELSE 'Unknown' END AS `Victim Descent`".format(
    ",".join("'{}'".format(k) for k in descent_mapping.keys()),
    "CASE `Vict Descent`" + "".join(" WHEN '{}' THEN '{}'".format(k, v) for k, v in descent_mapping.items()) + " END"
)

# Apply the mapping using expr
result_df = result_df.selectExpr("*", sql_expr)

# Rank the ZIP codes based on Median Household Income in descending order
income_rank_window = Window.orderBy(col("Median Household Income 2015").desc())
ranked_income_df = joined_df.select("Zip Code", "Median Household Income 2015").distinct().withColumn("Income Rank", rank().over(income_rank_window))

# Filter to get the top 3 richest ZIP codes
top3_richest_df = ranked_income_df.filter(col("Income Rank") <= 3)
bottom3_poorest_df = ranked_income_df.filter(col("Income Rank") > (ranked_income_df.count() - 3))

# Join the result_df with the top 3 richest ZIP codes
result_top3_df = result_df.join(top3_richest_df, "Zip Code").select("Victim Descent", "Victim Count")
result_bottom3_df = result_df.join(bottom3_poorest_df, "Zip Code").select("Victim Descent", "Victim Count")

# Group by Vict Descent and sum the counts
grouped_top3_result_df = result_top3_df.groupBy("Victim Descent").agg(sum("Victim Count").alias("#"))
grouped_bottom3_result_df = result_bottom3_df.groupBy("Victim Descent").agg(sum("Victim Count").alias("#"))

# Order by descending count
grouped_top3_result_df = grouped_top3_result_df.orderBy("#", ascending=False)
grouped_bottom3_result_df = grouped_bottom3_result_df.orderBy("#", ascending=False)

# Show the results
print("Top 3 ZIP Codes:")
grouped_top3_result_df.show(truncate=False)

print("Bottom 3 ZIP Codes:")
grouped_bottom3_result_df.show(truncate=False)

# Stop the Spark session
spark.stop()

