# -*- coding: utf-8 -*-
"""
   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Machine Learning - Clustering

The input data contains samples of cars and technical / price 
information about them. The goal of this problem is to group 
these cars into 4 clusters based on their attributes

## Techniques Used

1. K-Means Clustering
2. Centering and Scaling

-----------------------------------------------------------------------------
"""
#import os
#os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
#os.curdir

#Load the CSV file into a RDD
autoData = SpContext.textFile("auto-data.csv")
autoData.cache()

#Remove the first line (contains headers)
firstLine = autoData.first()
dataLines = autoData.filter(lambda x: x != firstLine)
dataLines.count()

from pyspark.sql import Row

import math
from pyspark.ml.linalg import Vectors

#Convert to Local Vector.
def transformToNumeric( inputStr) :
    attList=inputStr.split(",")

    doors = 1.0 if attList[3] =="two" else 2.0
    body = 1.0 if attList[4] == "sedan" else 2.0 
       
    #Filter out columns not wanted at this stage
    values= Row(DOORS= doors, \
                     BODY=float(body),  \
                     HP=float(attList[7]),  \
                     RPM=float(attList[8]),  \
                     MPG=float(attList[9])  \
                     )
    return values

autoMap = dataLines.map(transformToNumeric)
autoMap.persist()
autoMap.collect()

autoDf = SpSession.createDataFrame(autoMap)
autoDf.show()

#Centering and scaling. To perform this every value should be subtracted
#from that column's mean and divided by its Std. Deviation.

summStats=autoDf.describe().toPandas()
meanValues=summStats.iloc[1,1:5].values.tolist()
stdValues=summStats.iloc[2,1:5].values.tolist()

#place the means and std.dev values in a broadcast variable
bcMeans=SpContext.broadcast(meanValues)
bcStdDev=SpContext.broadcast(stdValues)

def centerAndScale(inRow) :
    global bcMeans
    global bcStdDev
    
    meanArray=bcMeans.value
    stdArray=bcStdDev.value

    retArray=[]
    for i in range(len(meanArray)):
        retArray.append( (float(inRow[i]) - float(meanArray[i])) /\
            float(stdArray[i]) )
    return Vectors.dense(retArray)
    
csAuto = autoDf.rdd.map(centerAndScale)
csAuto.collect()

#Create a Spark Data Frame
autoRows=csAuto.map( lambda f:Row(features=f))
autoDf = SpSession.createDataFrame(autoRows)

autoDf.select("features").show(10)

from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=3, seed=1)
model = kmeans.fit(autoDf)
predictions = model.transform(autoDf)
predictions.show()

#Plot the results in a scatter plot
import pandas as pd

def unstripData(instr) :
    return ( instr["prediction"], instr["features"][0], \
        instr["features"][1],instr["features"][2],instr["features"][3])
    
unstripped=predictions.rdd.map(unstripData)
predList=unstripped.collect()
predPd = pd.DataFrame(predList)

import matplotlib.pylab as plt
plt.cla()
plt.scatter(predPd[3],predPd[4], c=predPd[0])
