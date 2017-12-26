import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher


//This code is courtesy from
// https://github.com/harinij/100DaysOfCode/blob/master/Day%206%20-%20Spark%20Streaming%20using%20Scala/SparkStreamingTweet/src/com/hmovielabs/sparkstreaming/Utilities.scala

object Utilities {

  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  //Configures Twitter credential from twitter.txt

  def setupTwitter(): Unit ={
    import scala.io.Source

    for (line <- Source.fromFile("src/main/resources/twitter4j.properties").getLines){
      val fields = line.split(" ")
      if (fields.length==2)
        {
          System.setProperty("twitter4j.oauth" + fields(0), fields(1))
        }
    }
  }

  //Retreives a regex pattern for parsing Apache acces logs
  def apacheLogPattern():Pattern= {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }




}
