# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark SQL
-----------------------------------------------------------------------------
"""

#............................................................................
##   Working with Data Frames
#............................................................................

#Create a data frame from a JSON file
empDf = SpSession.read.json("customerData.json")
empDf.show()
empDf.printSchema()

#Do Data Frame queries
empDf.select("name").show()
empDf.filter(empDf["age"] == 40).show()
empDf.groupBy("gender").count().show()
empDf.groupBy("deptid").\
    agg({"salary": "avg", "age": "max"}).show()

#create a data frame from a list
 deptList = [{'name': 'Sales', 'id': "100"},\
     { 'name':'Engineering','id':"200" }]
 deptDf = SpSession.createDataFrame(deptList)
 deptDf.show()
 
#join the data frames
 empDf.join(deptDf, empDf.deptid == deptDf.id).show()
 
#cascading operations
empDf.filter(empDf["age"] >30).join(deptDf, \
        empDf.deptid == deptDf.id).\
        groupBy("deptid").\
        agg({"salary": "avg", "age": "max"}).show()
        
#............................................................................
##   Creating data frames from RDD
#............................................................................

from pyspark.sql import Row
lines = SpContext.textFile("auto-data.csv")
#remove the first line
datalines = lines.filter(lambda x: "FUELTYPE" not in x)
datalines.count()

parts = datalines.map(lambda l: l.split(","))
autoMap = parts.map(lambda p: Row(make=p[0],\
         body=p[4], hp=int(p[7])))
# Infer the schema, and register the DataFrame as a table.
autoDf = SpSession.createDataFrame(autoMap)
autoDf.show()

#............................................................................
##   Creating data frames directly from CSV
#...........................................................................
autoDf1 = SpSession.read.csv("auto-data.csv",header=True)
autoDf1.show()

#............................................................................
##   Creating and working with Temp Tables
#............................................................................

autoDf.createOrReplaceTempView("autos")
SpSession.sql("select * from autos where hp > 200").show()

#register a data frame as table and run SQL statements against it
empDf.createOrReplaceTempView("employees")
SpSession.sql("select * from employees where salary > 4000").show()

#to pandas data frame
#spark data frame is distributed across clusters
#pandas data frame is stored in the master node
empPands = empDf.toPandas()
for index, row in empPands.iterrows():
    print(row["salary"])

#............................................................................
##   Working with Databases
#............................................................................
#Make sure that the spark classpaths are set appropriately in the 
#spark-defaults.conf file to include the driver files
    
demoDf = SpSession.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/demo",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "demotable",
    user="root",
    password="").load()
    
demoDf.show()





