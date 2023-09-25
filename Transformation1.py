from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("C://Users/Consultant/Downloads/sample_data.csv", header=True, inferSchema=True)

# Select specific columns
selected_df = df.select("name", "age")

# Show the selected DataFrame
selected_df.show()
# Stop the SparkSession
spark.stop()
