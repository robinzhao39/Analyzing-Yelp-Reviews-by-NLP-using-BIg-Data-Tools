import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors


val sqlctx = new SQLContext(sc)


// read in from cleaned json file
val df = sqlctx.read.json("hdfs:///user/yz3919/project/cleaned_data/part-r-00000")


//tokenize the text
val tokenizer = new RegexTokenizer().setPattern("[\\W_]+").setMinTokenLength(4).setInputCol("review_text").setOutputCol("tokens")
val tokenized_df = tokenizer.transform(df)

//remove stop words
val stopwords = sc.textFile("project/tmp/stopwords").collect()

val stopwords_verb = Array("said","thank","foster","gave","recommend","like","ordered","went","told","called","love","know","came","work","worked","didn","make","makes","going","want","wanted","leave","left","away","took","take","come","need","needed","wasn","looking","used","review","think","check","look","looked","looking","worth","experience","getting","working","helped","tell","arrived","does","doing","thanks","having","had","love","visit","feel","asked","tried","loved","able")

val stopwords_adverbs = Array("actually","really","definitely","highly","extremely","pretty","quite","finally","just")

val stopwords_adjectives = Array("good","better","best","excellent","wonderful","perfect","amazing","super","nice","sweet","happy","awesome","great","friendly","right","sure","little")

val stopwords_nouns = Array("minutes","business","person","people","service","door","guys","night","stuff","thing","things","husband","time","food","place","store","price","prices","order","customer","staff","chicken","coffee","room","hair","home","appointment","shop","pizza","massage","years","burger","phone","breakfast","sushi","company","area","sandwich","life","cream","cheese","times","hour")

val new_stopwords = stopwords.union(stopwords_verb).union(stopwords_adverbs).union(stopwords_adjectives).union(stopwords_nouns)

val remover = new StopWordsRemover().setStopWords(new_stopwords).setInputCol("tokens").setOutputCol("filtered_tokens")
val all_rating_df = remover.transform(tokenized_df).withColumn("id", monotonically_increasing_id)
val high_rating_df = all_rating_df.filter("business_rating >= 4.5")
val low_rating_df = all_rating_df.filter("business_rating <= 2.5")




val all_rating_vectorizer = new CountVectorizer().setInputCol("filtered_tokens").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(all_rating_df)
val high_rating_vectorizer = new CountVectorizer().setInputCol("filtered_tokens").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(high_rating_df)
val low_rating_vectorizer = new CountVectorizer().setInputCol("filtered_tokens").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(low_rating_df)

val all_rating_countVectors = all_rating_vectorizer.transform(all_rating_df).select("id", "features")
val high_rating_countVectors = high_rating_vectorizer.transform(high_rating_df).select("id", "features")
val low_rating_countVectors = low_rating_vectorizer.transform(low_rating_df).select("id", "features")


val all_rating_ldaDf = all_rating_countVectors.map { case Row(id: Long, features: MLVector) => (id, Vectors.fromML(features))}.rdd
val high_rating_ldaDf = high_rating_countVectors.map { case Row(id: Long, features: MLVector) => (id, Vectors.fromML(features))}.rdd
val low_rating_ldaDf = low_rating_countVectors.map { case Row(id: Long, features: MLVector) => (id, Vectors.fromML(features))}.rdd


val all_rating_numTopics = 15
val high_rating_numTopics = 8
val low_rating_numTopics = 8


val all_rating_lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8)).setK(all_rating_numTopics).setMaxIterations(4).setDocConcentration(-1).setTopicConcentration(-1)
val high_rating_lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8)).setK(high_rating_numTopics).setMaxIterations(4).setDocConcentration(-1).setTopicConcentration(-1)
val low_rating_lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8)).setK(low_rating_numTopics).setMaxIterations(4).setDocConcentration(-1).setTopicConcentration(-1)


val all_rating_ldaModel = all_rating_lda.run(all_rating_ldaDf)
val high_rating_ldaModel = high_rating_lda.run(high_rating_ldaDf)
val low_rating_ldaModel = low_rating_lda.run(low_rating_ldaDf)


val all_rating_topicIndices = all_rating_ldaModel.describeTopics(maxTermsPerTopic = 5)
val high_rating_topicIndices = high_rating_ldaModel.describeTopics(maxTermsPerTopic = 5)
val low_rating_topicIndices = low_rating_ldaModel.describeTopics(maxTermsPerTopic = 5)


val all_rating_vocabList = all_rating_vectorizer.vocabulary
val high_rating_vocabList = high_rating_vectorizer.vocabulary
val low_rating_vocabList = low_rating_vectorizer.vocabulary

val all_rating_topics = all_rating_topicIndices.map { case (terms, termWeights) => terms.map(all_rating_vocabList(_)).zip(termWeights)}
val high_rating_topics = high_rating_topicIndices.map { case (terms, termWeights) => terms.map(high_rating_vocabList(_)).zip(termWeights)}
val low_rating_topics = low_rating_topicIndices.map { case (terms, termWeights) => terms.map(low_rating_vocabList(_)).zip(termWeights)}


println(s"\n\nall reviews topics")
all_rating_topics.zipWithIndex.foreach { case (topic, i) =>
  println(s"TOPIC $i")
  topic.foreach { case (term, weight) => println(s"$term\t$weight") }
  println(s"==========")
}


println(s"\n\n\nhigh business rating review topics")
high_rating_topics.zipWithIndex.foreach { case (topic, i) =>
  println(s"TOPIC $i")
  topic.foreach { case (term, weight) => println(s"$term\t$weight") }
  println(s"==========")
}

println(s"\n\n\nlow business rating review topics")
low_rating_topics.zipWithIndex.foreach { case (topic, i) =>
  println(s"TOPIC $i")
  topic.foreach { case (term, weight) => println(s"$term\t$weight") }
  println(s"==========")
}