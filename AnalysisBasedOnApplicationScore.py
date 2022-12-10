#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkConf, SparkContext

# This function just creates a Python "dictionary" we can later
# use to convert Sourcing Channel to Channel and Region  names while printing out
# the final results.
def loadSourcingChannel():
    insurancePremium = {}
    with open("ml-1000k/Hadoop_dataset/train.csv") as f:
        for line in f:
            fields = line.split(',')
            insurancePremium[str(fields[9])] = [fields[10],fields[9]]
    return insurancePremium


# In[ ]:


# Take each line of u.data and convert it to (Sourcing_Channel, (Application_underwritingscore, 1.0))


def parseInput(line):
    			if len(line)>0:	
                               fields = line.split(',')
           		       return (str(fields[9]), (float(fields[7]), 1.0))


# In[ ]:


if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("HighestApplicationscoreBySourcingChannel")
    sc = SparkContext(conf = conf)
    
    
    insurancePremium = loadSourcingChannel()

    # Load up the raw u.data file
    lines = sc.textFile("hdfs:///user/maria_dev/ml-1000k/train.csv")


# In[ ]:



    # Reduce to (Sourcing_Channel, (sutotalunderwritingscore, Customer))
    TotalsAndCount = Sourcing_Channel.reduceByKey(lambda Sourcing_Channel1, Sourcing_Channel2: ( Sourcing_Channel1[0] + Sourcing_Channel2[0], Sourcing_Channel1[1] + Sourcing_Channel2[1]) )


# In[ ]:


# Map to (rating, averageRating)
    averageRatings = TotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])


# In[ ]:


# Sort by average rating
    sortedSourcingChannel = averageRatings.sortBy(lambda x: x[1],False)

    # Take the top 10 results
    results = sortedSourcingChannel.take(20)

    # Print them out:
    for result in results:
        print(result[0],result[1])

