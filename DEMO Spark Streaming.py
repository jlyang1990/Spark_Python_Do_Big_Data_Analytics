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
#micro-batch size = 3(sec)
streamContext = StreamingContext(SpContext,3)

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
    
windowedRDD=lines.window(6,3)  #window size = 6(sec), i.e., last two micro-batches
windowedRDD.foreachRDD(windowMetrics)

streamContext.start()
#output every 3 seconds, including:
#1. word count within current RDD
#2. line count within current RDD and total line count
#3. total line count within current and previous RDD (i.e., window RDD)
streamContext.stop()
print("Overall lines :", totalLines)

