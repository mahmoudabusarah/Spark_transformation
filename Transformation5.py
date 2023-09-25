#Sorting by a column (order by age)

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SortByAge").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("C://Users/Consultant/Downloads/sample_data.csv", header=True, inferSchema=True)

# Sort the DataFrame by the "age" column in ascending order
sorted_df = df.orderBy("age")

# To sort in descending order, you can use the "desc" function
# sorted_df = df.orderBy("age", ascending=False)

# Show the sorted DataFrame
sorted_df.show()

# Stop the SparkSession
spark.stop()
