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

//SQL queries for descriptive statistics

sqlContext.sql("select * from df1").show()
// if what star ratings does customers give the most
sqlContext.sql("SELECT stars,count(*) FROM df2 group by stars").show()
// what category has the most number of businesses
sqlContext.sql("SELECT categories,count(*) FROM df1 group by categories").show()

sqlContext.sql("SELECT categories,count(*) FROM df1 group by categories").show()
