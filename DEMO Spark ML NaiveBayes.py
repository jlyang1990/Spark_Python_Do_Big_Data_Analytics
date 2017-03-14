# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------

           Naive Bayes : Spam Filtering
           
             Copyright : V2 Maestros @2016
                    
Problem Statement
*****************
The input data is a set of SMS messages that has been classified 
as either "ham" or "spam". The goal of the exercise is to build a
 model to identify messages as either ham or spam.

## Techniques Used

1. Naive Bayes Classifier
2. Training and Testing
3. Confusion Matrix
4. Text Pre-Processing
5. Pipelines

-----------------------------------------------------------------------------
"""
#import os
#os.chdir("C:/Personal/V2Maestros/Courses/Big Data Analytics with Spark/Python")
#os.curdir


"""--------------------------------------------------------------------------
Load Data
-------------------------------------------------------------------------"""
#Load the CSV file into a RDD
smsData = SpContext.textFile("SMSSpamCollection.csv",2)
smsData.cache()
smsData.collect()

"""--------------------------------------------------------------------------
Prepare data for ML
-------------------------------------------------------------------------"""

def TransformToVector(inputStr):
    attList=inputStr.split(",")
    smsType= 0.0 if attList[0] == "ham" else 1.0
    return [smsType, attList[1]]

smsXformed=smsData.map(TransformToVector)

smsDf= SpSession.createDataFrame(smsXformed,
                          ["label","message"])
smsDf.cache()
smsDf.select("label","message").show()

"""--------------------------------------------------------------------------
Perform Machine Learning
-------------------------------------------------------------------------"""
#Split training and testing
(trainingData, testData) = smsDf.randomSplit([0.9, 0.1])
trainingData.count()
testData.count()
testData.collect()

#Setup pipeline
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.feature import IDF

#Split into words and then build TF-IDF
tokenizer = Tokenizer(inputCol="message", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), \
        outputCol="tempfeatures")
idf=IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")
nbClassifier=NaiveBayes()

pipeline = Pipeline(stages=[tokenizer, hashingTF, \
                idf, nbClassifier])

#Build a model with a pipeline
nbModel=pipeline.fit(trainingData)
#Predict on test data (will automatically go through pipeline)
prediction=nbModel.transform(testData)

#Evaluate accuracy
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="label",metricName="accuracy")
evaluator.evaluate(prediction)

#Draw confusion matrics
prediction.groupBy("label","prediction").count().show()
