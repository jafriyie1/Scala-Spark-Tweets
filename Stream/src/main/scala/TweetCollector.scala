


//Collects Tweets and puts them in a mongodb for later processing
object TweetCollector {

  //import org.apache.hadoop.hbase._
  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  import org.apache.spark.streaming.twitter.TwitterUtils
  import twitter4j.auth.OAuthAuthorization
  import twitter4j.conf.ConfigurationBuilder
  import com.mongodb.spark._
  import org.bson.Document
  import Utilities._
  object TwitterMongo

  def main(args: Array[String]) {

    setupTwitter()
    val appName = "TwitterMongo"
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.set("spark.executor.memory","1g")
    conf.setAppName(appName)
    conf.set("spark.mongodb.output.uri","mongodb://127.0.0.1/twitter.net_tweets")
    conf.set("spark.mongodb.output.collection","net_tweets")
    conf.set("spark.mongodb.output.writeConcern.w","majority")
    conf.set("spark.mongodb.output.databaseName","twitter")
    val ssc = new StreamingContext(conf, Seconds(30))


    val tweets = TwitterUtils.createStream(ssc, None).filter(_.getLang == "en")

    val netTweets = tweets.filter(t=>{
      val electionT = t.getText.split(" ").map(_.toLowerCase())
      electionT.contains("tech") || electionT.contains("technology") || electionT.contains("bitcoin") || electionT.contains("AI") || electionT.contains("technology") || electionT.contains("Artifical Intelligence") || electionT.contains("robot")
    })

    val data = netTweets.map(tweet=>{
      val doc = new Document()
      doc.put("text",tweet.getText)
      doc.put("user",tweet.getUser.getName)
      doc.put("location",tweet.getUser.getLocation)
      doc.put("time",tweet.getCreatedAt)
      doc
    })

    data.foreachRDD(rdd=>{

      rdd.saveToMongoDB()

    })

    ssc.start()
    ssc.awaitTermination()

  }


}
