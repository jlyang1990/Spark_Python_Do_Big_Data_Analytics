# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------

                   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Demo : Spark Machine Learning - Linear Regression

Problem Statement
*****************
The input data set contains data about details of various car 
models. Based on the information provided, the goal is to come up 
with a model to predict Miles-per-gallon of a given model.

Techniques Used:

1. Linear Regression ( multi-variate)
2. Data Imputation - replacing non-numeric data with numeric ones
3. Variable Reduction - picking up only relevant features

-----------------------------------------------------------------------------
"""
#import os
#os.chdir("C:/Users/kumaran/Dropbox/V2Maestros/Courses/Spark n X - Do Big Data Analytics and ML/Python")
#os.curdir

"""--------------------------------------------------------------------------
Load Data
-------------------------------------------------------------------------"""

#Load the CSV file into a RDD
autoData = SpContext.textFile("auto-miles-per-gallon.csv")
autoData.cache()
autoData.take(5)
#Remove the first line (contains headers)
dataLines = autoData.filter(lambda x: "CYLINDERS" not in x)
dataLines.count()

"""--------------------------------------------------------------------------
Cleanup Data
-------------------------------------------------------------------------"""

from pyspark.sql import Row

#Use default for average HP
avgHP =SpContext.broadcast(80.0)

#Function to cleanup Data
def CleanupData( inputStr) :
    global avgHP
    attList=inputStr.split(",")
    
    #Replace ? values with a normal value
    hpValue = attList[3]
    if hpValue == "?":
        hpValue=avgHP.value
       
    #Create a row with cleaned up and converted data
    values= Row(     MPG=float(attList[0]),\
                     CYLINDERS=float(attList[1]), \
                     DISPLACEMENT=float(attList[2]), 
                     HORSEPOWER=float(hpValue),\
                     WEIGHT=float(attList[4]), \
                     ACCELERATION=float(attList[5]), \
                     MODELYEAR=float(attList[6]),\
                     NAME=attList[7]  ) 
    return values

#Run map for cleanup
autoMap = dataLines.map(CleanupData)
autoMap.cache()
autoMap.take(5)
#Create a Data Frame with the data. 
autoDf = SpSession.createDataFrame(autoMap)

"""--------------------------------------------------------------------------
Perform Data Analytics
-------------------------------------------------------------------------"""
#See descriptive analytics.
autoDf.select("MPG","CYLINDERS").describe().show()


#Find correlation between predictors and target
for i in autoDf.columns:
    if not( isinstance(autoDf.select(i).take(1)[0][0], unicode)) :
        print( "Correlation to MPG for ", i, autoDf.stat.corr('MPG',i))


"""--------------------------------------------------------------------------
Prepare data for ML
-------------------------------------------------------------------------"""

#Transform to a Data Frame for input to Machine Learing
#Drop columns that are not required (low correlation)

from pyspark.ml.linalg import Vectors
def transformToLabeledPoint(row) :
    lp = ( row["MPG"], Vectors.dense([row["ACCELERATION"],\
                        row["DISPLACEMENT"], \
                        row["WEIGHT"]]))
    return lp
    
autoLp = autoMap.map(transformToLabeledPoint)
autoDF = SpSession.createDataFrame(autoLp,["label", "features"])
autoDF.select("label","features").show(10)


"""--------------------------------------------------------------------------
Perform Machine Learning
-------------------------------------------------------------------------"""

#Split into training and testing data
(trainingData, testData) = autoDF.randomSplit([0.9, 0.1])
trainingData.count()
testData.count()

#Build the model on training data
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(maxIter=10)
lrModel = lr.fit(trainingData)

#Print the metrics
print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))

#Predict on the test data
predictions = lrModel.transform(testData)
predictions.select("prediction","label","features").show()

#Find R2 for Linear Regression
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="label",metricName="r2")
evaluator.evaluate(predictions)
