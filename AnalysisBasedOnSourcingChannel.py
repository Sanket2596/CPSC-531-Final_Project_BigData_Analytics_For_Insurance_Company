#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkConf, SparkContext

# the final results.
def loadSourcingChannel():
    insurancePremium = {}
    with open("ml-1000k/Hadoop_dataset/train.csv") as f:
        for line in f:
            fields = line.split(',')
            insurancePremium[str(fields[9])] = [fields[10],fields[9]]
    return insurancePremium


# In[ ]:


# Take each line of u.data and convert it to (Sourcing_Channel, (Insurance, 1.0))
def parseInput(line):
    			if len(line)>0:	
                               fields = line.split(',')
           		       return (str(fields[9]), (int(fields[11]), 1.0))
                             


if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("HighestPremiumCapturedBySourcingChannel")
    sc = SparkContext(conf = conf)

   
    insurancePremium = loadSourcingChannel()


# In[ ]:


# Load up the raw u.data file
    lines = sc.textFile("hdfs:///user/maria_dev/ml-1000k/train.csv")

    # Convert to (Sourcing_Channel, (Insurance, 1.0))
    Sourcing_Channel = lines.map(parseInput)


# In[ ]:


# Reduce to (Sourcing_Channe, (totalRevenue, NoofCustomer))
    TotalsAndCount = Sourcing_Channel.reduceByKey(lambda Sourcing_Channel1, Sourcing_Channel2: ( Sourcing_Channel1[0] + Sourcing_Channel2[0], Sourcing_Channel1[1] + Sourcing_Channel2[1] ) )

    # Map to (rating, averageRating)
    averageRatings = TotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])


# In[ ]:


# Sort by average rating
   sortedSourcing= averageRatings.sortBy(lambda x: x[1],False)

   # Take the top 10 results
   results = sortedSourcing.take(5)

   # Print them out:
   for result in results:
       print(insurancePremium[result[0]], result[1])

