import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# Step 1: Extraction
spark = SparkSession.builder.appName("FriendsETL2").getOrCreate()
dataframe = spark.read.csv("D:\DataScience/SparkCourse/ETL/fakefriends-header.csv",\
                           header= True,\
                           sep=",")

print('Total Records = {}'.format(dataframe.count()))
dataframe.show()


# Step 2: Transformation

byage = dataframe.groupBy("age").count()
byage.show()

dataframe.registerTempTable("adultFriends")
adult = spark.sql('SELECT * from adultFriends WHERE age > 30')

print ("adult table is:")
adult.show()
print('Total count = {}'.format(adult.count()))


aggregate = spark.sql('SELECT COUNT(*) as count, age from adultFriends GROUP BY age')
aggregate.show()


# Step 3: Loading

# writeto mysql
aggregate.write.format('jdbc').options(
        #url='jdbc:mysql://localhost/dbName',
        url='jdbc:mysql://localhost/friends',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='count',
        user='root',
        password='Baha123456789@').mode('append').save()


# read from dataBase
df = spark.read.format('jdbc').options(
        #url='jdbc:mysql://localhost/dbName',
        url='jdbc:mysql://localhost/friends',
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='count',
        user='root',
        password='Baha123456789@').load()


print("df")
df.orderBy('age').show()