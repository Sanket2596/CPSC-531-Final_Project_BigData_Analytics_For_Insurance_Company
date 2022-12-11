#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit


def loadSourcingChannel():
    insurancePremium = {}
    with open("ml-1000k/Hadoop_dataset/train.csv") as f:
        for line in f:
            fields = line.split(',')
            insurancePremium[str(fields[9])] = [fields[10],fields[9]]
    return insurancePremium


# In[ ]:


# Convert u.data lines into (SourcingChannel, applicationscore, premium) rows
def parseInput(line):
    fields = line.value.split()
    return Row(SourcingChannel= int(fields[9]), applicationScore= int(fields[7]), rating = int(fields[11]))


# In[ ]:


if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("InsuranceRecs").getOrCreate()

    # This line is necessary on HDP 2.6.5:
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    # Load up our movie ID -> name dictionary
    InsuranceChannel = loadSourcingChannel()

    # Get the raw data
    lines = spark.read.text("ml-1000k/Hadoop_dataset/train.csv").rdd


# In[ ]:


# Convert it to a RDD of Row objects with (SourcingChannel, applicationScore, premium)
    insuranceRDD = lines.map(parseInput)

    # Convert to a DataFrame and cache it
    insurance = spark.createDataFrame(insuranceRDD).cache()


# In[ ]:


# Create an ALS collaborative filtering model from the complete data set
   als = ALS(maxIter=5, regParam=0.01, userCol="SourcingChannel", itemCol="applicationScore", ratingCol="premium")
   model = als.fit(ratings)


# In[ ]:


# Print out ratings from user 0:
   print("\nRatings for user ID 0:")
   userPremium = insurance.filter("SourcingChannel= C")
   for insurance in userPremium.collect():
       print InsuranceChannel[insurance['applicationScore']], rating['premium']

   print("\npremium recommendations:")
      
   insurancePremium = insurance.select("applicationScore").withColumn('SourcingChannel', lit(C))

  
   recommendations = model.transform(insurancePremium)

  
   topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

   for recommendation in topRecommendations:
       print (InsuranceChannel[recommendation['applicationScore']], recommendation['prediction'])

   spark.stop()

