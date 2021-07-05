import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


# Step 1: Extraction
spark = SparkSession.builder.appName("Friends").getOrCreate()

schema = StructType([ \
                     StructField("UserID", IntegerType(), True), \
                     StructField("Name", StringType(), True), \
                     StructField("Age", IntegerType(), True),\
                     StructField("Friends", IntegerType(), True)])

dataframe = spark.read.schema(schema).csv("D:\DataScience/SparkCourse/ETL/fakefriends.csv")
dataframe.show()

# Step 2: Transformation

#example 1
print("age >= 20")
adult = dataframe[dataframe['Age']>=20]
adult.show()

#example 2
print("orderbyAge")
orderbyAge= dataframe.orderBy('age')
orderbyAge.show()

#example 3
print("groupbyAge")
groupbyAge = dataframe.groupBy("age").count()
groupbyAge.show()


# Step 3: Loading
# Load to mysql
import mysql.connector

db_connection = mysql.connector.connect(user="root", password="Baha123456789@")
db_cursor = db_connection.cursor()
db_cursor.execute("DROP DATABASE IF EXISTS Friends;")
db_cursor.execute("CREATE DATABASE Friends;")
db_cursor.execute("USE Friends;")
db_cursor.execute("CREATE TABLE adultFriends(UserID INT, \
                                             Name VARCHAR(10), \
                                             Age INT,\
                                             Friends INT);")

for x in adult.collect():
    q = "INSERT INTO adultFriends (UserID, Name, Age, Friends) VALUES (%s, %s, %s, %s)"
    a = tuple(x)
    db_cursor.execute(q, a)
db_connection.commit()

db_cursor.execute('SELECT * FROM adultFriends;')
result = db_cursor.fetchmany(5)
for row in result:
    print(row)

