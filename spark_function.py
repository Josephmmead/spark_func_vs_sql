from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import avg, min, col, rank
from pyspark.sql.window import Window


# settubg up SparkSessiong
spark = SparkSession.builder.getOrCreate()

# Stating schema for data
schema = StructType([StructField("name", StringType(), True),
                     StructField("index", StringType(), True),
                     StructField("gender", StringType(), True),
                     StructField("age", StringType(), True),
                     StructField("country", StringType(), True),
                     StructField("state", StringType(), True),
                     StructField("salary", StringType(), True)])

# Grabbing data from csv to generate a dataframe
df = spark.read.csv('./data/demographics.csv', schema=schema)

# Create a SparkSession
spark = SparkSession.builder \
    .appName("PySpark Functions Example") \
    .getOrCreate()

# Sample data in a list of dictionaries
data = [
    {"name": "john_doe", "index": "001", "gender": "male", "age": 52, "country": "USA", "state": "Alabama", "salary": 52000},
    {"name": "jane_doe", "index": "002", "gender": "female", "age": 25, "country": "USA", "state": "California", "salary": 93250},
    {"name": "hans_solo", "index": "003", "gender": "male", "age": 48, "country": "Corellia", "state": "NA", "salary": 1000000},
    {"name": "darth_vader", "index": "004", "gender": "male", "age": 44, "country": "Tatooine", "state": "NA", "salary": 9999999999},
]

# Create a DataFrame from the data
df = spark.createDataFrame(data)

# Show the DataFrame
df.show()

# Function 1: Selecting columns
selected_columns = df.select("name", "age", "salary")
print("selected_columns")
selected_columns.show()

'''
output: 
+--------+-------+-------+
| country|  state|min_age|
+--------+-------+-------+
|Corellia|     NA|     48|
|Tatooine|     NA|     44|
|     USA|Alabama|     52|
+--------+-------+-------+
'''

# Function 2: Filtering data
male_employees = df.filter(col("gender") == "male")
print("male_employees")
male_employees.show()

'''
output: 
+--------+-------+-------+
| country|  state|min_age|
+--------+-------+-------+
|Corellia|     NA|     48|
|Tatooine|     NA|     44|
|     USA|Alabama|     52|
+--------+-------+-------+
'''

# Function 3: Grouping and Aggregation
avg_salary_per_country_state = df.groupBy("country", "state").agg(avg("salary").alias("avg_salary"))
print("avg_salary_per_country_state")
avg_salary_per_country_state.show()

'''
output: 
+--------+-------+-------+
| country|  state|min_age|
+--------+-------+-------+
|Corellia|     NA|     48|
|Tatooine|     NA|     44|
|     USA|Alabama|     52|
+--------+-------+-------+
'''

# Function 4: Finding the youngest male employee in each country and state combination
window_spec = Window.partitionBy("country", "state").orderBy("age")
youngest_male_per_country_state = df.filter(col("gender") == "male") \
    .withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") == 1) \
    .drop("rank")
print("youngest_male_per_country_state")
youngest_male_per_country_state.show()

'''
output: 
+--------+-------+-------+
| country|  state|min_age|
+--------+-------+-------+
|Corellia|     NA|     48|
|Tatooine|     NA|     44|
|     USA|Alabama|     52|
+--------+-------+-------+
'''

# Function 5: Joining DataFrames
joined_df = df.join(avg_salary_per_country_state, on=["country", "state"], how="inner")
print("joined_df")
joined_df.show()

'''
output: 
+--------+-------+-------+
| country|  state|min_age|
+--------+-------+-------+
|Corellia|     NA|     48|
|Tatooine|     NA|     44|
|     USA|Alabama|     52|
+--------+-------+-------+
'''

# Stop the SparkSession
spark.stop()
