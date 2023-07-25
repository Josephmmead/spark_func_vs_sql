from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

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

# Register the DataFrame as a temporary SQL table (or view)
df.createOrReplaceTempView("employees")

# Function 1: Selecting columns
selected_columns = spark.sql("SELECT name, age, salary FROM employees")
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
male_employees = spark.sql("SELECT * FROM employees WHERE gender = 'male'")
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
avg_salary_per_country_state = spark.sql("SELECT country, state, AVG(salary) AS avg_salary FROM employees GROUP BY country, state")
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
youngest_male_per_country_state = spark.sql("""
    SELECT country, state, MIN(age) AS min_age
    FROM employees
    WHERE gender = 'male'
    GROUP BY country, state
""")
print("youngest_male_per_country_state")
youngest_male_per_country_state.show()

'''
output: 
+-------+-------+-------+
|country|  state|min_age|
+-------+-------+-------+
|Germany|Bavaria|     48|
| Sweden|Gotland|     44|
|    USA|Alabama|     52|
+-------+-------+-------+
'''

# Function 5: Joining DataFrames using SQL JOIN
joined_df = spark.sql("""
    SELECT e.*, a.avg_salary
    FROM employees e
    INNER JOIN (
        SELECT country, state, AVG(salary) AS avg_salary
        FROM employees
        GROUP BY country, state
    ) a
    ON e.country = a.country AND e.state = a.state
""")
print("joined_df")
joined_df.show()

'''
output: 
+---------------+-----+------+---+-------+----------+------+----------+
|           name|index|gender|age|country|     state|salary|avg_salary|
+---------------+-----+------+---+-------+----------+------+----------+
| lars_sigardson|  004|  male| 44| Sweden|   Gotland| 75000|   75000.0|
|     mary_smith|  005|female| 18|England|    Dorset|110000|  110000.0|
|    hans_ulrich|  003|  male| 48|Germany|   Bavaria|120030|  120030.0|
|jessica_O'Brian|  006|female| 23|    USA|California| 64025|   78637.5|
|       jane_doe|  002|female| 25|    USA|California| 93250|   78637.5|
|       john_doe|  001|  male| 52|    USA|   Alabama| 52000|   52000.0|
+---------------+-----+------+---+-------+----------+------+----------+
'''

# Stop the SparkSession
spark.stop()


