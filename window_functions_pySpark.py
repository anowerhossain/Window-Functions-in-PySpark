from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, rank, dense_rank, 
    first, last, lead, lag
)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WindowFunctions") \
    .getOrCreate()

# Sample data
data = [
    (1, "North", 500),
    (2, "North", 500),
    (3, "North", 400),
    (4, "South", 700),
    (5, "South", 700),
    (6, "South", 600)
]

# Define schema
columns = ["salesperson_id", "region", "sales_amount"]

# Create DataFrame
sales_df = spark.createDataFrame(data, schema=columns)

# Define the window specification
window_spec = Window.partitionBy("region").orderBy("sales_amount")

# Define the window spec with an explicit range
window_spec_full = Window.partitionBy("region").orderBy("sales_amount").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# 1. Row Number
sales_df = sales_df.withColumn("row_number", row_number().over(window_spec))

# 2. Rank
sales_df = sales_df.withColumn("rank", rank().over(window_spec))

# 3. Dense Rank
sales_df = sales_df.withColumn("dense_rank", dense_rank().over(window_spec))

# 4. Lead
sales_df = sales_df.withColumn("lead_value", lead("sales_amount", 1).over(window_spec))

# 5. Lag
sales_df = sales_df.withColumn("lag_value", lag("sales_amount", 1).over(window_spec))

# 6. First Value
sales_df = sales_df.withColumn("first_value", first("sales_amount").over(window_spec))

# 7. Last Value
sales_df = sales_df.withColumn("last_value", last("sales_amount").over(window_spec_full))


# Show the results
sales_df.show()
