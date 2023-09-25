#Adding a new column with a conditional expression( if age >30 "Yes" else "No")


from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Create a SparkSession
spark = SparkSession.builder.appName("AddConditionalColumn").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("C://Users/Consultant/Downloads/sample_data.csv", header=True, inferSchema=True)

# Add a new column "IsOver30" based on the "age" column
df_with_conditional_column = df.withColumn("IsOver30", when(col("age") > 30, "Yes").otherwise("No"))

# Show the DataFrame with the new column
df_with_conditional_column.show()

# Stop the SparkSession
spark.stop()
