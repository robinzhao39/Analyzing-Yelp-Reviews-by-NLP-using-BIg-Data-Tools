
'''
Standardize columns for t test
for i in ["sentiment","stars"]:
    assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
    scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
    pipeline = Pipeline(stages=[assembler, scaler])
    df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")

print("After Scaling :")
df.show(5)

'''


import pandas as pd
df1=pd.read_json('yelp_academic_dataset_review.json', lines=True)



df = spark.read.json("yelp_project/yelp_academic_dataset_review.json")


import pandas as pd
df1=pd.read_json('yelp_academic_dataset_review.json', lines=True)
for i in df1['Text']:
    blob = TextBlob(i)



from pyspark.sql.functions import from_unixtime, to_date, year, udf, explode, split, col, length, rank, dense_rank, avg, sum
sentiment = udf(lambda x: TextBlob(x).sentiment[0])
spark.udf.register(“sentiment”, sentiment)
tweets = tweets.withColumn(‘sentiment’,sentiment(‘text’).cast(‘double’))







from textblob import TextBlob
from pyspark.sql.functions import from_unixtime, to_date, year, udf, explode, split, col, length, rank, dense_rank, avg, sum
spark = SparkSession.builder\
    .config("spark.jars", "/home/bz1037/.local/lib/python3.7/site-packages/textblob/lib/textblob.jar")\
    .getOrCreate()
df = spark.read.json("yelp_project/yelp_academic_dataset_review.json")
sentiment = udf(lambda x: TextBlob(x).sentiment[0])
spark.udf.register("sentiment", sentiment)
df=df.withColumn('sentiment',sentiment('Text').cast('double'))
df.select('sentiment').show()



pip install pypi-install import the library via pypi in databricks
upload module to worker nodes

spark = SparkSession.builder \
    .master("local[*]")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nly_2.11:2.4.5")\
    .config("spark.kryoserializer.buffer.max", "1G")\
    .getOrCreate()



import sparknlp
spark=sparknlp.start()
from pyspark.sql import SparkSession
spark = SparkSession.builder\
    .config("spark.jars", "/home/bz1037/.local/lib/python3.7/site-packages/sparknlp/lib/sparknlp.jar")\
    .getOrCreate()
from sparknlp.pretrained import PretrainedPipeline
pipeline = PretrainedPipeline('explain_document_ml', lang = 'en')
annotations =  pipeline.fullAnnotate(""Hello from John Snow Labs ! "")[0]
annotations.keys()


from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, year, udf, explode, split, col, length, rank, dense_rank, avg, sum
if __name__ == "__main__":
    # create Spark session
    df = spark.read.json("yelp_project/yelp_academic_dataset_review.json")
    pyspark.SparkContext.addPyFile("/home/bz1037/.local/lib/python3.7/site-packages/textblob/__init__.py")
    sentiment = udf(lambda x: TextBlob(x).sentiment[0])
    spark.udf.register("sentiment", sentiment)
    df=df.withColumn('sentiment',sentiment('Text').cast('double'))
    df.select('sentiment').show()

int_rdd.map(lambda x: TextBlob(x).sentiment[0]).collect()


df.agg(avg(col('stars')))