# import packages

import textblob
from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, avg, col, round
from pyspark import SparkContext
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

#read in data
df = spark.read.json("yelp_project/yelp_academic_dataset_review.json")
sc = SparkContext.getOrCreate();
sc.addPyFile(textblob.__file__)

# Create user defined function using textblob
sentiment = udf(lambda x: TextBlob(x).sentiment[0])
spark.udf.register("sentiment", sentiment)

#process sentiment
df=df.withColumn('sentiment',round(sentiment('Text').cast('double'),2))
df.select('sentiment','stars').show()

# Next, we want to see how difference are predicted sentiment and actualy ratings. 
df.agg(avg(col('stars'))).collect()
#3.73

df.registerTempTable("df");
sqlContext = SQLContext(sc)
sqlContext.sql("SELECT avg(sentiment) from df").show()
#0.235


