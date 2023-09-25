from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("AddSalaryColumn").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("C://Users/Consultant/Downloads/sample_data.csv", header=True, inferSchema=True)

# Add a new column "SalaryPlus10000"
df_with_new_column = df.withColumn("SalaryPlus10000", col("Salary") + 10000)

# Show the DataFrame with the new column
df_with_new_column.show()

# Stop the SparkSession
spark.stop()
