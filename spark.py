from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import subjectivity
from nltk.sentiment import SentimentAnalyzer
from nltk.sentiment.util import *
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
import os
import datetime
import json
import subprocess


# set environment variable PYSPARK_SUBMIT_ARGS
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/StevenSYT/Documents/Academic/18_Spring/Big_Data/Homework/HW3/Elastic_Kibana/elasticsearch-hadoop-6.2.3/dist/elasticsearch-spark-20_2.11-6.2.3.jar pyspark-shell'



TCP_IP = 'localhost'
TCP_PORT = 3000

# Pyspark
# # create spark configuration
conf = SparkConf()
# conf.setAppName('TwitterApp')
# conf.setMaster('local[4]')
# create spark context with the above configuration
sc = SparkContext(conf=conf)

# use accumulator for id generation
accum = 0

# create the Streaming Context from spark context with interval size 6 seconds
ssc = StreamingContext(sc, 8)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9001
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

# create a sentiment analyzer
sid = SentimentIntensityAnalyzer()

# connect to our cluster
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es_write_conf = {
# specify the node that we are sending data to (this should be the master)
"es.nodes" : 'localhost',

# specify the port in case it is not the default port
"es.port" : '9200',

# specify a resource in the form 'index/doc-type'
"es.resource" : 'tweetmap/tweet',

# is the input JSON?
"es.input.json" : "yes",

# # is there a field in the mapping that should be used to specify the ES document ID
# "es.mapping.id": "doc_id"
}

mappings = {
    "mappings": {
    "tweet": {
      "properties": {
        "location": {
          "type": "geo_point"
        }
      }
    }
  }
}

es.indices.create(index='tweetmap', body=mappings)
######### your processing here ###################
dataStream.pprint()



# sentiment analysis [tweet,"location.latitude,location.longitude", str(UID)]
def senAnalysis(line):
   ss = sid.polarity_scores(line[0])
   result = line[0] + ' -- has '
   if ss['compound'] > 0:
      sentiment = 'positive'
      result += 'positive sentiment \n'
   elif ss['compound'] == 0:
      sentiment = 'neutral'
      result += 'neutral sentiment \n'
   else:
      sentiment = 'negative'
      result += 'negative sentiment \n'
   return (result, (line[0], sentiment, line[1], line[2]))

# formatData from senResults
def formatData(line):
   jsonLike = {'tweet' : line[1][0], 'sentiment' : line[1][1], 'location' : line[1][2], 'timeStamp' : str(datetime.datetime.now().isoformat())}

   return (line[1][3], json.dumps(jsonLike))

def sendDataToES(rdd):
   rdd.saveAsNewAPIHadoopFile(
   path='',
   outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
   keyClass="org.apache.hadoop.io.NullWritable",
   valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",

   # critically, we must specify our `es_write_conf`
   conf=es_write_conf)

# generate the lineArray
tweets = dataStream.map(lambda x: x.split("\t"))

#senResults contains (printresults, (tweet, sentiment, geolocation, timeStamp))
senResults = tweets.map(senAnalysis)

# print out the results of sentiment
senScores = senResults.map(lambda x: x[0])
senScores.pprint()

# map the OutPut json format for ES input
tweetOutPut = senResults.map(formatData)
tweetOutPut.pprint()

#send data to ES
tweetOutPut.foreachRDD(sendDataToES)

# tweetTexts.pprint()
# words = tweets.flatMap(lambda x: x[0].split(' '))
# wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
# wordcount.pprint()


#################################################

ssc.start()
ssc.awaitTermination()

# subprocess, shell daemon
