# -*- coding: utf-8 -*-
"""
Make sure you give execute privileges
-----------------------------------------------------------------------------

           Spark with Python: Setup Spyder IDE for Spark

             Copyright : V2 Maestros @2016
                    
Execute this script once when Spyder is started on Windows
-----------------------------------------------------------------------------
"""

import os
import sys

# NOTE: Please change the folder paths to your current setup.
#Windows
if sys.platform.startswith('win'):
    #Where you downloaded the resource bundle
    os.chdir("C:/Users/Kumaran/Dropbox/V2Maestros/Modules/Apache Spark/Python")
    #Where you installed spark.    
    os.environ['SPARK_HOME'] = 'C:/Spark/spark-2.0.0-bin-hadoop2.7'
#other platforms - linux/mac
else:
    os.chdir("/Users/jlyang/Documents/Intern&Job/Spark_Python_Do_Big_Data_Analytics")
    os.environ['SPARK_HOME'] = '/Users/jlyang/Spark/spark-2.1.0-bin-hadoop2.7'

os.curdir

# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']

#Add the following paths to the system path. Please check your installation
#to make sure that these zip files actually exist. The names might change
#as versions change.
sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","pyspark.zip"))
sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib","py4j-0.10.4-src.zip"))

#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("jlyang_spark") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","2") \
    .config("spark.sql.warehouse.dir", "/Users/jlyang/Spark/spark-warehouse")\
    .getOrCreate()
    
#Get the Spark Context from Spark Session    
SpContext = SpSession.sparkContext

#Test Spark
testData = SpContext.parallelize([3,6,4,2])
testData.count()
#check http://localhost:4040 to see if Spark is running


