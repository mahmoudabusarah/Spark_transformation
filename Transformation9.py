from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.appName("UnionDataFrames").getOrCreate()

# Read the CSV file into the first DataFrame
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Salary", IntegerType(), True)
])

df = spark.read.csv("C:/Users/Consultant/Downloads/sample_data.csv", header=True, schema=schema)

# Create the second DataFrame df2
df2 = spark.createDataFrame([("Mike", 40, 80000)], ["Name", "Age", "Salary"])

# Union the two DataFrames
combined_df = df.union(df2)

# Show the combined DataFrame
combined_df.show()

# Stop the SparkSession
spark.stop()
