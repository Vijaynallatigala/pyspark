from pyspark import SparkConf
from pyspark.sql import SparkSession
import os

from pyspark.sql.functions import when,col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

os.environ[ "PYSPARK_PYTHON"] = "C:/Users/User/Documents/Python/Python37/python.exe"

#1st way to create spark session
"""
spark = SparkSession.builder.appName("vijay").master("local[*]").getOrCreate()
df =(spark.read
     .format("csv")
     .option("header", True)
     
     \
     .option("path","E://data/details.csv").load()
     )
df.show()
"""
#schema defining
#schema_ddl = "id INT, Name STRING, Salary INT, City String"

# Define schema programmatically

schema = StructType([
StructField("id", IntegerType(), True),
StructField("Name", StringType(), True),
StructField("Salary", IntegerType(), True),
StructField("City", StringType(), True)
])

#2nd way to create spark session

# create sparkconf object
conf = SparkConf()
conf.set("spark.app.name", "pysparkprogram")
conf.set("spark.master", "local[*]")
conf.set("spark.executor.memory", "2g") #memory value should be a string

# use conf in sparksession.builder
spark =SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.format("csv").schema(schema).option("path","E://data/details.csv").load()
df.show()

""""
df.select(
      col("id"),
      col("Name"),
      col("Salary"),
      col("City"),
      when(col("salary")<800,"poor").
      when((col("salary") >800) & (col("salary") <= 1000),
     "rich").otherwise("ultrarich").alias("status")).show()
"""



# using spark sql with spark session


#df.createOrReplaceTempView("vijay")
#result = spark.sql("select * from vijay where id > 5")
#result.show()

df.withColumn("Status",
              when(col("Salary") > 800, "rich")
              .when((col("Salary") > 400) & (col("Salary") <= 800), "middle")
              .otherwise("poor")
              )\
     .withColumn("category",
                when(col("id")> 5, "senior").otherwise("junior")
                )
df.show()



""
