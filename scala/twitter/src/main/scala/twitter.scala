/* Finder.scala */

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object Twitter {

    def main(args: Array[String]) {
      // Create our configuration.
      val conf = new SparkConf().setAppName("Twitter")
      val sc = new SparkContext(conf)
      //sc.setLogLevel("WARN")

      val ssc = new StreamingContext(sc, Seconds(5))

      System.setProperty("twitter4j.oauth.consumerKey", "***********************")
      System.setProperty("twitter4j.oauth.consumerSecret", "***********************")
      System.setProperty("twitter4j.oauth.accessToken", "***********************")
      System.setProperty("twitter4j.oauth.accessTokenSecret", "***********************")

      val stream = TwitterUtils.createStream(ssc, None)
      val hashTags = stream.filter(status => {
        var result = false
        for (hashtag <- status.getHashtagEntities) if (hashtag.getText == "PokemonGO") result = true
        result
      })
      val hashTagPairs = hashTags.map(status => "\n==>" + status.getText)
      hashTagPairs.foreachRDD(rdd => rdd.collect().foreach{case text => println(text)})
      ssc.start()
      ssc.awaitTermination()
    }
}
