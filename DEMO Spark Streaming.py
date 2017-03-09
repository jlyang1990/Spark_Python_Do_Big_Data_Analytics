# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Streaming
-----------------------------------------------------------------------------
"""

from pyspark.streaming import StreamingContext


#............................................................................
##   Streaming with TCP/IP data
#............................................................................

#Create streaming context with latency of 1
streamContext = StreamingContext(SpContext,3)

totalLines=0
lines = streamContext.socketTextStream("localhost", 9000)


#Word count within RDD    
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint(5)

#Count lines
totalLines=0
linesCount=0
def computeMetrics(rdd):
    global totalLines
    global linesCount
    linesCount=rdd.count()
    totalLines+=linesCount
    print(rdd.collect())
    print("Lines in RDD :", linesCount," Total Lines:",totalLines)

lines.foreachRDD(computeMetrics)

#Compute window metrics
def windowMetrics(rdd):
    print("Window RDD size:", rdd.count())
    
windowedRDD=lines.window(6,3)
windowedRDD.foreachRDD(windowMetrics)

streamContext.start()
streamContext.stop()
print("Overall lines :", totalLines)

