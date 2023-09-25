from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create a SparkSession
spark = SparkSession.builder.appName("GroupByAge").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("C://Users/Consultant/Downloads/sample_data.csv", header=True, inferSchema=True)

# Group by "age" and calculate the average salary for each age group
result_df = df.groupBy("age").agg(avg(col("Salary")).alias("AverageSalary"))

# Show the resulting DataFrame
result_df.show()

# Stop the SparkSession
spark.stop()
