
#Dropping a column (drop salary)
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("DropColumn").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("C://Users/Consultant/Downloads/sample_data.csv", header=True, inferSchema=True)

# Drop the "Salary" column
df_without_salary = df.drop("Salary")

# Show the DataFrame without the "Salary" column
df_without_salary.show()

# Stop the SparkSession
spark.stop()
