# -*- coding: utf-8 -*-
"""
   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Samples : Spark Machine Learning - Decision Trees

Problem Statement
*****************
The input data contains surveyed information about potential 
customers for a bank. The goal is to build a model that would 
predict if the prospect would become a customer of a bank, 
if contacted by a marketing exercise.

## Techniques Used

1. Random Forests
2. Training and Testing
3. Confusion Matrix
4. Indicator Variables
5. Variable Reduction
6. Principal Component Analysis

-----------------------------------------------------------------------------
"""
#import os
#os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
#os.curdir

"""--------------------------------------------------------------------------
Load Data
-------------------------------------------------------------------------"""
#Load the CSV file into a RDD
bankData = SpContext.textFile("bank.csv")
bankData.cache()
bankData.count()

#Remove the first line (contains headers)
firstLine=bankData.first()
dataLines = bankData.filter(lambda x: x != firstLine)
dataLines.count()

"""--------------------------------------------------------------------------
Cleanup Data
-------------------------------------------------------------------------"""

# Change labels to numeric ones and build a Row object

import math
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors

def transformToNumeric( inputStr) :
    
    attList=inputStr.replace("\"","").split(";")
    
    age=float(attList[0])
    #convert outcome to float    
    outcome = 0.0 if attList[16] == "no" else 1.0
    
    #create indicator variables for single/married    
    single= 1.0 if attList[2] == "single" else 0.0
    married = 1.0 if attList[2] == "married" else 0.0
    divorced = 1.0 if attList[2] == "divorced" else 0.0
    
    #create indicator variables for education
    primary = 1.0 if attList[3] == "primary" else 0.0
    secondary = 1.0 if attList[3] == "secondary" else 0.0
    tertiary = 1.0 if attList[3] == "tertiary" else 0.0
    
    #convert default to float
    default= 0.0 if attList[4] == "no" else 1.0
    #convert balance amount to float
    balance=float(attList[5])
    #convert loan to float
    loan= 0.0 if attList[7] == "no" else 1.0
    
    #Create a row with cleaned up and converted data
    values= Row(     OUTCOME=outcome ,\
                    AGE=age, \
                    SINGLE=single, \
                    MARRIED=married, \
                    DIVORCED=divorced, \
                    PRIMARY=primary, \
                    SECONDARY=secondary, \
                    TERTIARY=tertiary, \
                    DEFAULT=default, \
                    BALANCE=balance, \
                    LOAN=loan                    
                    ) 
    return values
    
#Change to a Vector
bankRows = dataLines.map(transformToNumeric)
bankRows.collect()[:15]

bankData = SpSession.createDataFrame(bankRows)

"""--------------------------------------------------------------------------
Perform Data Analytics
-------------------------------------------------------------------------"""
#See descriptive analytics.
bankData.describe().show()

#Find correlation between predictors and target
for i in bankData.columns:
    if not( isinstance(bankData.select(i).take(1)[0][0], unicode)) :
        print( "Correlation to OUTCOME for ", i, \
            bankData.stat.corr('OUTCOME',i))

"""--------------------------------------------------------------------------
Prepare data for ML
-------------------------------------------------------------------------"""
#Transform to a Data Frame for input to Machine Learing

def transformToLabeledPoint(row) :
    lp = ( row["OUTCOME"], \
            Vectors.dense([
                row["AGE"], \
                row["BALANCE"], \
                row["DEFAULT"], \
                row["DIVORCED"], \
                row["LOAN"], \
                row["MARRIED"], \
                row["PRIMARY"], \
                row["SECONDARY"], \
                row["SINGLE"], \
                row["TERTIARY"]
        ]))
    return lp
    
bankLp = bankData.rdd.map(transformToLabeledPoint)
bankLp.collect()
bankDF = SpSession.createDataFrame(bankLp,["label", "features"])
bankDF.select("label","features").show(10)

"""--------------------------------------------------------------------------
Perform Machine Learning
-------------------------------------------------------------------------"""

#Perform PCA
from pyspark.ml.feature import PCA
bankPCA = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
pcaModel = bankPCA.fit(bankDF)
pcaResult = pcaModel.transform(bankDF).select("label","pcaFeatures")
pcaResult.show(truncate=False)

#Indexing needed as pre-req for Decision Trees
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
si_model = stringIndexer.fit(pcaResult)
td = si_model.transform(pcaResult)
td.collect()

#Split into training and testing data
(trainingData, testData) = td.randomSplit([0.7, 0.3])
trainingData.count()
testData.count()
testData.collect()

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


#Create the model
rmClassifer = RandomForestClassifier(labelCol="indexed", \
                featuresCol="pcaFeatures")
rmModel = rmClassifer.fit(trainingData)

#Predict on the test data
predictions = rmModel.transform(testData)
predictions.select("prediction","indexed","label","pcaFeatures").show()
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="indexed",metricName="accuracy")
evaluator.evaluate(predictions)      

#Draw a confusion matrix
predictions.groupBy("indexed","prediction").count().show()


