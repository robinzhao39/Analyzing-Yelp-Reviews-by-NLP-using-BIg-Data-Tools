// import packages
val sc: SparkContext // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
val input_file1= "yelp_project/yelp_academic_dataset_business.json"
val input_file2= "yelp_project/yelp_academic_dataset_review.json"
//Create dataframe
val df1 = sqlContext.read.json(input_file1)
val df2 = sqlContext.read.json(input_file2)
df1.registerTempTable("df1")
df2.registerTempTable("df2")


// Pull out reviews with hot topics extracted from ML

//High ratings topics: fresh, park, delicious, owner, quality

// extract reviewa with 'fresh', sorted by number of stars descending.
sqlContext.sql("SELECT text FROM df2 where text like '%fresh%' order by stars desc limit 20").show()


// extract reviewa with 'fresh', sorted by number of stars descending.
sqlContext.sql("SELECT text FROM df2 where text like '%park%' order by stars desc limit 20").show()


//Low ratings topics include: rude, worst, problem, small, location

sqlContext.sql("SELECT text FROM df2 where text like '%small%' order by stars limit 20").show()

sqlContext.sql("SELECT text FROM df2 where text like '%rude%' order by stars limit 20").show()