import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.bson.Document
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession






object Model {

  case class Tweets(id: String, text: String, user: String, location: String, time: String)

  def main(args: Array[String]): Unit = {


   val appName = "twitter"
   /* val sparkSession = SparkSession.builder().appName(appName)
      .master("local[*]")
      .config("spark.executor.memory","1g")

      .config("spark.mongodb.input.uri","mongodb://127.0.0.1/twitter.net_tweets")
      .config("spark.mongodb.output.uri","mongodb://127.0.0.1/twitter.net_tweets")
      .getOrCreate()

    val readConfigs = ReadConfig(Map("uri"->"mongodb://127.0.0.1/twitter.net_tweets?readPreference=primaryPreferred"))

*/
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.set("spark.executor.memory","1g")
    conf.setAppName(appName)
    conf.set("spark.mongodb.input.uri","mongodb://127.0.0.1/twitter.net_tweets")
    conf.set("spark.mongodb.input.collection","net_tweets")
    conf.set("spark.mongodb.input.writeConcern.w","majority")
    conf.set("spark.mongodb.input.databaseName","twitter")
    //val ssc = new StreamingContext(conf, Seconds(30))
    val sc = new SparkContext(conf)

    val rdd  = MongoSpark.load(sc)

    val sqlContext = SQLContext.getOrCreate(sc)
    val df = MongoSpark.load(sqlContext).toDF()

    val data = rdd.toDF()
    //val data = MongoSpark.load(sparkSession).toDF()
    println(rdd.count)
    //println(rdd.foreach())
    data.printSchema()







  }


}
