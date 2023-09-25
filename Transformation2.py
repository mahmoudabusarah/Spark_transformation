from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("C://Users/Consultant/Downloads/sample_data.csv", header=True, inferSchema=True)

filtered_df = df.filter(df["age"] > 30)

# Alternatively, you can use the "where" method:
# filtered_df = df.where(df["age"] > 30)

# Show the filtered DataFrame
filtered_df.show()

# Stop the SparkSession
spark.stop()
