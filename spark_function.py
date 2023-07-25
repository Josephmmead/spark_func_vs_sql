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


# Function 1: Selecting columns
selected_columns = df.select("name", "age", "salary")
print("selected_columns")
selected_columns.show()

'''
output: 
+---------------+---+------+
|           name|age|salary|
+---------------+---+------+
|       john_doe| 52| 52000|
|       jane_doe| 25| 93250|
|    hans_ulrich| 48|120030|
| lars_sigardson| 44| 75000|
|     mary_smith| 18|110000|
|jessica_O'Brian| 23| 64025|
+---------------+---+------+
'''

# Function 2: Filtering data
male_employees = df.filter(col("gender") == "male")
print("male_employees")
male_employees.show()

'''
output: 
+--------------+-----+------+---+-------+-------+------+
|          name|index|gender|age|country|  state|salary|
+--------------+-----+------+---+-------+-------+------+
|      john_doe|  001|  male| 52|    USA|Alabama| 52000|
|   hans_ulrich|  003|  male| 48|Germany|Bavaria|120030|
|lars_sigardson|  004|  male| 44| Sweden|Gotland| 75000|
+--------------+-----+------+---+-------+-------+------+
'''

# Function 3: Grouping and Aggregation
avg_salary_per_country_state = df.groupBy("country", "state").agg(avg("salary").alias("avg_salary"))
print("avg_salary_per_country_state")
avg_salary_per_country_state.show()

'''
output: 
+-------+----------+----------+
|country|     state|avg_salary|
+-------+----------+----------+
| Sweden|   Gotland|   75000.0|
|England|    Dorset|  110000.0|
|Germany|   Bavaria|  120030.0|
|    USA|California|   78637.5|
|    USA|   Alabama|   52000.0|
+-------+----------+----------+
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
+--------------+-----+------+---+-------+-------+------+
|          name|index|gender|age|country|  state|salary|
+--------------+-----+------+---+-------+-------+------+
|   hans_ulrich|  003|  male| 48|Germany|Bavaria|120030|
|lars_sigardson|  004|  male| 44| Sweden|Gotland| 75000|
|      john_doe|  001|  male| 52|    USA|Alabama| 52000|
+--------------+-----+------+---+-------+-------+------+
'''

# Function 5: Joining DataFrames
joined_df = df.join(avg_salary_per_country_state, on=["country", "state"], how="inner")
print("joined_df")
joined_df.show()

'''
output: 
+-------+----------+---------------+-----+------+---+------+----------+
|country|     state|           name|index|gender|age|salary|avg_salary|
+-------+----------+---------------+-----+------+---+------+----------+
| Sweden|   Gotland| lars_sigardson|  004|  male| 44| 75000|   75000.0|
|England|    Dorset|     mary_smith|  005|female| 18|110000|  110000.0|
|Germany|   Bavaria|    hans_ulrich|  003|  male| 48|120030|  120030.0|
|    USA|California|jessica_O'Brian|  006|female| 23| 64025|   78637.5|
|    USA|California|       jane_doe|  002|female| 25| 93250|   78637.5|
|    USA|   Alabama|       john_doe|  001|  male| 52| 52000|   52000.0|
+-------+----------+---------------+-----+------+---+------+----------+
'''

# Stop the SparkSession
spark.stop()
