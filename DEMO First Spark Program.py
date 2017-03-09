# -*- coding: utf-8 -*-
"""
Make sure you give execute privileges
-----------------------------------------------------------------------------

           Spark with Python: Your first Spark Program

             Copyright : V2 Maestros @2016
                    
Please make sure to execute "SETUP Python for Spark.py" before running this
-----------------------------------------------------------------------------
"""

#Create an RDD by loading from a file
tweetsRDD = SpContext.textFile("movietweets.csv")

#show top 5 records
tweetsRDD.take(5)

#Transform Data - change to upper Case
ucRDD = tweetsRDD.map( lambda x : x.upper() )
ucRDD.take(5)

#Action - Count the number of tweets
tweetsRDD.count()

