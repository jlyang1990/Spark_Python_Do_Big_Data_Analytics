# -*- coding: utf-8 -*-
"""
Make sure you give execute privileges
-----------------------------------------------------------------------------

           Spark with Python: APPLY Project Solution

             Copyright : V2 Maestros @2016
                    
Solution for the APPLY project in this course
-----------------------------------------------------------------------------
"""
#
#os.chdir("C:\\Users\\kumaran\\Dropbox\\V2Maestros\\Courses\\Spark n X - Do Big Data Analytics and ML\\Python")

"""-----------------------------------------------------------------------------
Load Data from the data file
--------------------------------------------------------------------------"""
#Load the file into a RDD
ccRaw = SpContext.textFile("credit-card-default-1000.csv")
ccRaw.take(5)

#Remove header row
dataLines = ccRaw.filter(lambda x: "EDUCATION" not in x)
dataLines.count()
dataLines.take(1000)

"""-----------------------------------------------------------------------------
Prepare and augment the data
--------------------------------------------------------------------------"""

#Cleanup data. Remove lines that are not "CSV"
filteredLines = dataLines.filter(lambda x : x.find("aaaaaa") < 0 )
filteredLines.count()

#Remove double quotes that are present in few records.
cleanedLines = filteredLines.map(lambda x: x.replace("\"", ""))
cleanedLines.count()
cleanedLines.cache()

#Convert into SQL Dataframe. In the process perform few cleanups and 
#changes required for future work.
from pyspark.sql import Row

def convertToRow(instr) :
    attList = instr.split(",")
 
    # PR#06 Round of age to range of 10s.    
    ageRound = round(float(attList[5]) / 10.0) * 10
    
    #Normalize sex to only 1 and 2.
    sex = attList[2]
    if sex =="M":
        sex=1
    elif sex == "F":
        sex=2
    
    #Find average billed Amount. You can either continue with individual
    #amounts or use the average for future predictions
    avgBillAmt = (float(attList[12]) +  \
                    float(attList[13]) + \
                    float(attList[14]) + \
                    float(attList[15]) + \
                    float(attList[16]) + \
                    float(attList[17]) ) / 6.0
                    
    #Find average pay amount
    avgPayAmt = (float(attList[18]) +  \
                    float(attList[19]) + \
                    float(attList[20]) + \
                    float(attList[21]) + \
                    float(attList[22]) + \
                    float(attList[23]) ) / 6.0
                    
    #Find average pay duration. Required for PR#04
    #Make sure numbers are rounded and negative values are eliminated
    avgPayDuration = round((abs(float(attList[6])) + \
                        abs(float(attList[7])) + \
                        abs(float(attList[8])) +\
                        abs(float(attList[9])) +\
                        abs(float(attList[10])) +\
                        abs(float(attList[11]))) / 6)
    
    #Average percentage paid. add this as an additional field to see
    #if this field has any predictive capabilities. This is 
    #additional creative work that you do to see possibilities.                    
    perPay = round((avgPayAmt/(avgBillAmt+1) * 100) / 25) * 25
                    
    values = Row (  CUSTID = attList[0], \
                    LIMIT_BAL = float(attList[1]), \
                    SEX = float(sex),\
                    EDUCATION = float(attList[3]),\
                    MARRIAGE = float(attList[4]),\
                    AGE = float(ageRound), \
                    AVG_PAY_DUR = float(avgPayDuration),\
                    AVG_BILL_AMT = abs(float(avgBillAmt)), \
                    AVG_PAY_AMT = float(avgPayAmt), \
                    PER_PAID= abs(float(perPay)), \
                    DEFAULTED = float(attList[24]) 
                    )

    return values

#Cleanedup RDD    
ccRows = cleanedLines.map(convertToRow)
ccRows.take(60)
#Create a data frame.
ccDf = SpSession.createDataFrame(ccRows)
ccDf.cache()
ccDf.show(10)

#Enhance Data
import pandas as pd

#Add SEXNAME to the data using SQL Joins. Required for PR#02
genderDict = [{"SEX" : 1.0, "SEX_NAME" : "Male"}, \
                {"SEX" : 2.0, "SEX_NAME" : "Female"}]                
genderDf = SpSession.createDataFrame(pd.DataFrame(genderDict, \
            columns=['SEX', 'SEX_NAME']))
genderDf.collect()
ccDf1 = ccDf.join( genderDf, ccDf.SEX== genderDf.SEX ).drop(genderDf.SEX)
ccDf1.take(5)

#Add ED_STR to the data with SQL joins. Required for PR#03
eduDict = [{"EDUCATION" : 1.0, "ED_STR" : "Graduate"}, \
                {"EDUCATION" : 2.0, "ED_STR" : "University"}, \
                {"EDUCATION" : 3.0, "ED_STR" : "High School" }, \
                {"EDUCATION" : 4.0, "ED_STR" : "Others"}]                
eduDf = SpSession.createDataFrame(pd.DataFrame(eduDict, \
            columns=['EDUCATION', 'ED_STR']))
eduDf.collect()
ccDf2 = ccDf1.join( eduDf, ccDf1.EDUCATION== eduDf.EDUCATION ).drop(eduDf.EDUCATION)
ccDf2.take(5)

#Add MARR_DESC to the data. Required for PR#03
marrDict = [{"MARRIAGE" : 1.0, "MARR_DESC" : "Single"}, \
                {"MARRIAGE" : 2.0, "MARR_DESC" : "Married"}, \
                {"MARRIAGE" : 3.0, "MARR_DESC" : "Others"}]                
marrDf = SpSession.createDataFrame(pd.DataFrame(marrDict, \
            columns=['MARRIAGE', 'MARR_DESC']))
marrDf.collect()
ccFinalDf = ccDf2.join( marrDf, ccDf2.MARRIAGE== marrDf.MARRIAGE ).drop(marrDf.MARRIAGE)
ccFinalDf.cache()
ccFinalDf.take(5)

"""-----------------------------------------------------------------------------
Do analysis as required by the problem statement
--------------------------------------------------------------------------"""

#Create a temp view
ccFinalDf.createOrReplaceTempView("CCDATA")

#PR#02 solution
SpSession.sql("SELECT SEX_NAME, count(*) as Total, " + \
                " SUM(DEFAULTED) as Defaults, " + \
                " ROUND(SUM(DEFAULTED) * 100 / count(*)) as PER_DEFAULT " + \
                "FROM CCDATA GROUP BY SEX_NAME"  ).show()

#PR#03 solution                
SpSession.sql("SELECT MARR_DESC, ED_STR, count(*) as Total," + \
                " SUM(DEFAULTED) as Defaults, " + \
                " ROUND(SUM(DEFAULTED) * 100 / count(*)) as PER_DEFAULT " + \
                "FROM CCDATA GROUP BY MARR_DESC,ED_STR " + \
                "ORDER BY 1,2").show()
#PR#04 solution                
SpSession.sql("SELECT AVG_PAY_DUR, count(*) as Total, " + \
                " SUM(DEFAULTED) as Defaults, " + \
                " ROUND(SUM(DEFAULTED) * 100 / count(*)) as PER_DEFAULT " + \
                "FROM CCDATA GROUP BY AVG_PAY_DUR ORDER BY 1"  ).show()

#Perform first round Correlation analysis
for i in ccDf.columns:
    if not( isinstance(ccDf.select(i).take(1)[0][0], str)) :
        print( "Correlation to DEFAULTED for ", i,\
            ccDf.stat.corr('DEFAULTED',i))


"""-----------------------------------------------------------------------------
Transform to a Data Frame for input to Machine Learing
--------------------------------------------------------------------------"""

import math
from pyspark.ml.linalg import Vectors

def transformToLabeledPoint(row) :
    lp = ( row["DEFAULTED"], \
            Vectors.dense([
                row["AGE"], \
                row["AVG_BILL_AMT"], \
                row["AVG_PAY_AMT"], \
                row["AVG_PAY_DUR"], \
                row["EDUCATION"], \
                row["LIMIT_BAL"], \
                row["MARRIAGE"], \
                row["PER_PAID"], \
                row["SEX"]
        ]))
    return lp
    
ccLp = ccFinalDf.rdd.repartition(2).map(transformToLabeledPoint)
ccLp.collect()
ccNormDf = SpSession.createDataFrame(ccLp,["label", "features"])
ccNormDf.select("label","features").show(10)
ccNormDf.cache()


#Indexing needed as pre-req for Decision Trees
from pyspark.ml.feature import StringIndexer
stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
si_model = stringIndexer.fit(ccNormDf)
td = si_model.transform(ccNormDf)
td.collect()

#Split into training and testing data
(trainingData, testData) = td.randomSplit([0.7, 0.3])
trainingData.count()
testData.count()

"""-----------------------------------------------------------------------------
PR#05 Do Predictions - to predict defaults. Use multiple classification
algorithms to see which ones provide the best results
--------------------------------------------------------------------------"""

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                    labelCol="indexed",metricName="accuracy")

#Create the Decision Trees model
dtClassifer = DecisionTreeClassifier(labelCol="indexed", \
                featuresCol="features")
dtModel = dtClassifer.fit(trainingData)
#Predict on the test data
predictions = dtModel.transform(testData)
predictions.select("prediction","indexed","label","features").collect()
print("Results of Decision Trees : ",evaluator.evaluate(predictions))      

#Create the Random Forest model
rmClassifer = RandomForestClassifier(labelCol="indexed", \
                featuresCol="features")
rmModel = rmClassifer.fit(trainingData)
#Predict on the test data
predictions = rmModel.transform(testData)
predictions.select("prediction","indexed","label","features").collect()
print("Results of Random Forest : ",evaluator.evaluate(predictions)  )

#Create the Naive Bayes model
nbClassifer = NaiveBayes(labelCol="indexed", \
                featuresCol="features")
nbModel = nbClassifer.fit(trainingData)
#Predict on the test data
predictions = nbModel.transform(testData)
predictions.select("prediction","indexed","label","features").collect()
print("Results of Naive Bayes : ",evaluator.evaluate(predictions)  )


"""-----------------------------------------------------------------------------
PR#06 Group data into 4 groups based on the said parameters
--------------------------------------------------------------------------"""
#Filter only columns needed for clustering
ccClustDf = ccFinalDf.select("SEX","EDUCATION","MARRIAGE","AGE","CUSTID")

#Do centering and scaling for the values
summStats=ccClustDf.describe().toPandas()
meanValues=summStats.iloc[1,1:5].values.tolist()
stdValues=summStats.iloc[2,1:5].values.tolist()
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
    return Row(CUSTID=inRow[4], features=Vectors.dense(retArray))
    
ccMap = ccClustDf.rdd.repartition(2).map(centerAndScale)
ccMap.collect()

#Create a Spark Data Frame with the features
ccFClustDf = SpSession.createDataFrame(ccMap)
ccFClustDf.cache()

ccFClustDf.select("features").show(10)

#Perform clustering
from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=4, seed=1)
model = kmeans.fit(ccFClustDf)
predictions = model.transform(ccFClustDf)
predictions.select("*").show()

"""-----------------------------------------------------------------------------
Get creative and try out various other things you wish to. 
More practice means more confidence. Best of luck.
--------------------------------------------------------------------------"""

