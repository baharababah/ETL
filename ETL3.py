import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import mysql.connector
from pyspark.sql import functions as func

# Step 1: Extraction
spark = SparkSession.builder.appName("UsedCars").getOrCreate()


dataframe = spark.read.csv("D:\DataScience/SparkCourse/ETL/UsedCars.csv", \
                                          header = True,
                                           sep=",")

dataframe.show(3)


# Step 2: Transformation
split_colMileage = func.split(dataframe['Mileage'], ' ')
dataframe = dataframe.withColumn('Mileage', split_colMileage.getItem(0))

split_colEngine = func.split(dataframe['Engine'], ' ')
dataframe = dataframe.withColumn('Engine', split_colEngine.getItem(0))

split_colPower = func.split(dataframe['Power'], ' ')
dataframe = dataframe.withColumn('Power', split_colPower.getItem(0))


split_colNewPrice = func.split(dataframe['New_Price'], ' ')
dataframe = dataframe.withColumn('New_Price', split_colNewPrice.getItem(0))

dataframe.show(3)

byFuelType = dataframe.groupBy("Fuel_Type").count()
byFuelType.show()


byName = dataframe.groupBy("Name").sum()
byName.show()

# Step 3: Loading

db_connection = mysql.connector.connect(user="root", password="XXXX")
db_cursor = db_connection.cursor()
db_cursor.execute("DROP DATABASE IF EXISTS Vehicles;")
db_cursor.execute("CREATE DATABASE Vehicles;")
db_cursor.execute("USE Vehicles;")
db_cursor.execute('CREATE TABLE cars (ID INT, \
                                             Name VARCHAR(128), \
                                             Location VARCHAR(32),\
                                             Year INT,\
                                             Kilometers_Driven INT,\
                                             Fuel_Type VARCHAR(10),\
                                             Transmission VARCHAR(10),\
                                             Owner_Type VARCHAR(10),\
                                             Mileage INT,\
                                             Engine INT,\
                                             Power FLOAT,\
                                             Seats INT,\
                                             New_Price FLOAT);')

for x in dataframe.collect():
    q = "INSERT INTO cars (ID, Name, location, Year, Kilometers_Driven, Fuel_Type, \
    Transmission, Owner_Type, Mileage, Engine, Power, Seats, New_Price) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    a = tuple(x)
    db_cursor.execute(q, a)
db_connection.commit()

# select
data = db_cursor.execute("SELECT *FROM Sonar;")
data.show()