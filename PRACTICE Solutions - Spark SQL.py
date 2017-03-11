"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
PRACTICE Exercises : Spark SQL
-----------------------------------------------------------------------------
"""

"""
-----------------------------------------------------------------------------
#Spark Data Frames
******************

Your course resource has a CSV file "iris.csv". 
Load that file into a Spark SQL Data Frame called "irisDF".

Hint: You need to use RDDs and remove the header line.
-----------------------------------------------------------------------------
"""
# Note : Spark SQL does not provide a direct way to load a csv into
# a DF. You need to first create an RDD and then load into a DF
 
irisRDD = SpContext.textFile("iris.csv")
#Remove the header line
irisData = irisRDD.filter(lambda x: "Sepal" not in x)
irisData.count()

#Split the columns
cols = irisData.map(lambda l : l.split(","))
#Make row objects
from pyspark.sql import Row
irisMap = cols.map( lambda p: Row ( SepalLengh = p[0], \
                                   SepalWidth = p[1], \
                                   PetalLength = p[2], \
                                   PetalWidth = p[3], \
                                   Species = p[4] ))
irisMap.collect()
#Create a data frame from the Row objects
irisDF = SpSession.createDataFrame(irisMap)
irisDF.select("*").show()

# Directly load a csv file into a DF
irisDF = SpSession.read.csv("iris.csv", header = True)
irisDF = irisDF.withColumnRenamed("Sepal.Length", "SepalLength")\
.withColumnRenamed("Sepal.Width", "SepalWidth")\
.withColumnRenamed("Petal.Length", "PetalLength")\
.withColumnRenamed("Petal.Width", "PetalWidth")
irisDF.show()

"""
-----------------------------------------------------------------------------
In the irisDF, filter for rows whose PetalWidth is greater than 0.4
and count them.
Hint: Check for Spark documentation on how to count rows : 
https://spark.apache.org/docs/latest/api/python/pyspark.sql.html
-----------------------------------------------------------------------------
"""
irisDF.filter( irisDF["PetalWidth"] > 0.4).count()    
    
"""
-----------------------------------------------------------------------------
#Spark SQL Temp Tables
***********************

 Register a temp table called "iris" using irisDF. Then find average
Petal Width by Species using that table.
-----------------------------------------------------------------------------
"""
irisDF.createOrReplaceTempView("iris")
SpSession.sql("select Species, avg(PetalWidth) from iris group by Species")\
.show()

"""
-----------------------------------------------------------------------------
Hope you had some good practice !! Recommend trying out your own use cases
-----------------------------------------------------------------------------
"""
    



