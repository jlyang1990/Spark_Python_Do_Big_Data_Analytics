# -*- coding: utf-8 -*-
"""
   Spark with Python

             Copyright : V2 Maestros @2016
                    
Code Demo : Spark Machine Learning - Decision Trees

Problem Statement
*****************
The input data is the iris dataset. It contains recordings of 
information about flower samples. For each sample, the petal and 
sepal length and width are recorded along with the type of the 
flower. We need to use this dataset to build a decision tree 
model that can predict the type of flower based on the petal 
and sepal information.

## Techniques Used

1. Decision Trees 
2. Training and Testing
3. Confusion Matrix

-----------------------------------------------------------------------------
"""
#import os
#os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
#os.curdir

"""--------------------------------------------------------------------------
Load Data
-------------------------------------------------------------------------"""

#Load the CSV file into a RDD
irisData = SpContext.textFile("iris.csv")
irisData.cache()
irisData.count()

#Remove the first line (contains headers)
dataLines = irisData.filter(lambda x: "Sepal" not in x)
dataLines.count()

"""--------------------------------------------------------------------------
Cleanup Data
-------------------------------------------------------------------------"""

from pyspark.sql import Row
#Create a Data Frame from the data
parts = dataLines.map(lambda l: l.split(","))
irisMap = parts.map(lambda p: Row(SEPAL_LENGTH=float(p[0]),\
                                SEPAL_WIDTH=float(p[1]), \
                                PETAL_LENGTH=float(p[2]), \
                                PETAL_WIDTH=float(p[3]), \
                                SPECIES=p[4] ))
                                
# Infer the schema, and register the DataFrame as a table.
irisDf = SpSession.createDataFrame(irisMap)
irisDf.cache()

#Add a numeric indexer for the label/target column
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="SPECIES", outputCol="IND_SPECIES")
si_model = stringIndexer.fit(irisDf)
irisNormDf = si_model.transform(irisDf)

irisNormDf.select("SPECIES","IND_SPECIES").distinct().show()
irisNormDf.cache()

"""--------------------------------------------------------------------------
Perform Data Analytics
-------------------------------------------------------------------------"""

#See standard parameters
irisNormDf.describe().show()

#Find correlation between predictors and target
for i in irisNormDf.columns:
    if not( isinstance(irisNormDf.select(i).take(1)[0][0], unicode)) :
        print( "Correlation to Species for ", i, \
                    irisNormDf.stat.corr('IND_SPECIES',i))

"""--------------------------------------------------------------------------
Prepare data for ML
-------------------------------------------------------------------------"""

#Transform to a Data Frame for input to Machine Learing
#Drop columns that are not required (low correlation)

from pyspark.ml.linalg import Vectors
def transformToLabeledPoint(row) :
    lp = ( row["SPECIES"], row["IND_SPECIES"], \
                Vectors.dense([row["SEPAL_LENGTH"],\
                        row["SEPAL_WIDTH"], \
                        row["PETAL_LENGTH"], \
                        row["PETAL_WIDTH"]]))
    return lp
    
irisLp = irisNormDf.rdd.map(transformToLabeledPoint)
irisLpDf = SpSession.createDataFrame(irisLp,["species","label", "features"])
irisLpDf.select("species","label","features").show(10)
irisLpDf.cache()

"""--------------------------------------------------------------------------
Perform Machine Learning
-------------------------------------------------------------------------"""
#Split into training and testing data
(trainingData, testData) = irisLpDf.randomSplit([0.9, 0.1])
trainingData.count()
testData.count()
testData.show()

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#Create the model
dtClassifer = DecisionTreeClassifier(maxDepth=2, labelCol="label",\
                featuresCol="features")
dtModel = dtClassifer.fit(trainingData)

dtModel.numNodes
dtModel.depth

#Predict on the test data
predictions = dtModel.transform(testData)
predictions.select("prediction","species","label").show()

#Evaluate accuracy
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="label",metricName="accuracy")
evaluator.evaluate(predictions)      

#Draw a confusion matrix
predictions.groupBy("label","prediction").count().show()

