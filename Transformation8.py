#Renaming columns (name to full name)

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("RenameColumn").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("C://Users/Consultant/Downloads/sample_data.csv", header=True, inferSchema=True)

# Rename the "name" column to "full name"
df_with_renamed_column = df.withColumnRenamed("name", "full name")

# Show the DataFrame with the renamed column
df_with_renamed_column.show()

# Stop the SparkSession
spark.stop()
