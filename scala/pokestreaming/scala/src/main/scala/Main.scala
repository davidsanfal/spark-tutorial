import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

import org.apache.spark._
import org.apache.spark.streaming._


case class Position(latitude: Double, longitude: Double){
  override def toString(): String = "(" + latitude + ", " + longitude + ")";
  def -(that: Position): Double = {
    val earthRadius = 6371000; //meters
    val dLat = Math.toRadians(this.latitude - that.latitude)
    val dLng = Math.toRadians(this.longitude - that.longitude)
    val a = Math.sin(dLat/2) * Math.sin(dLat/2) + Math.cos(Math.toRadians(this.latitude)) * Math.cos(Math.toRadians(that.latitude)) * Math.sin(dLng/2) * Math.sin(dLng/2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
    earthRadius * c
   }
}
case class Pokemon(position: Position, readed: Long, id: Long, pokemonId: Long){
  override def toString(): String = "[position: " + position + ", readed: " + readed + ", id" + id + ", pokemonId: " + pokemonId + "]";
}


object Pokedex {
  def elder = (p1:Pokemon, p2:Pokemon) => if (p1.readed < p2.readed) p2 else p1

  def alive(p:Tuple2[Long, Pokemon], t:Long): Boolean = p._2.readed > t

  def main(args: Array[String]) {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var pokemons = Source.fromFile("..\\data\\pokedex.txt").getLines().toList.map(_.split(",")).map(x => x(0).toLong -> x(1)).toMap

    val conf = new SparkConf().setAppName("Pokedex")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 8888)
    val data = lines.map(x => x.split(","))
    //val last_readed = data.map(p => p(5).toLong).reduce((x, y) => if(x > y) x else y).take(1)

    val maped = data.map(p => (p(0).toLong, new Pokemon(new Position(p(2).toDouble,
                                                                     p(3).toDouble),
                                                        p(4).toLong,
                                                        p(0).toLong,
                                                        p(1).toLong)))
    //val filtered = maped.filter(alive(_, last_readed))
    val reduced = maped.reduceByKey((p1, p2) => elder(p1, p2))
    val mapresult = reduced.map(p => (pokemons(p._2.pokemonId) ,(p._2.position - new Position(40.4499529, -3.6897580999999993))))
    val result = mapresult.reduceByKey((a, b) => if(a < b) a else b)
    result.foreachRDD(rdd => rdd.collect().foreach{case p => println(p)})

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}

/*

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.Row

import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec


case class Position(latitude: Double, longitude: Double){
  override def toString(): String = "(" + latitude + ", " + longitude + ")";
  def -(that: Position): Double = {
    val earthRadius = 6371000; //meters
    val dLat = Math.toRadians(this.latitude - that.latitude)
    val dLng = Math.toRadians(this.longitude - that.longitude)
    val a = Math.sin(dLat/2) * Math.sin(dLat/2) + Math.cos(Math.toRadians(this.latitude)) * Math.cos(Math.toRadians(that.latitude)) * Math.sin(dLng/2) * Math.sin(dLng/2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a))
    earthRadius * c
   }
}
case class Pokemon(position: Position, readed: Long, id: Long, pokemonId: Long){
  override def toString(): String = "[position: " + position + ", readed: " + readed + ", id" + id + ", pokemonId: " + pokemonId + "]";
}


object Pokedex {
    def elder = (p1:Pokemon, p2:Pokemon) => if (p1.readed < p2.readed) p2 else p1

    def alive(p:Tuple2[Long, Pokemon], t:Long): Boolean = p._2.readed > t
    def main(args: Array[String]) {
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
      var pokemons = Source.fromFile("..\\data\\pokedex.txt").getLines().toList.map(_.split(",")).map(x => x(0).toLong -> x(1)).toMap

      // Create our configuration.
      val conf = new SparkConf().setAppName("Pokedex")
      val sc = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val df = sqlContext.read.json("..\\data\\data*.json")
      val last_readed = df.map(p => p.getAs[Long]("readed")).reduce((x, y) => if(x > y) x else y)

      val maped = df.map(p => (p.getAs[Long]("id"), new Pokemon(new Position(p.getAs[Double]("latitude"),
                                                                             p.getAs[Double]("longitude")),
                                                                p.getAs[Long]("readed"),
                                                                p.getAs[Long]("id"),
                                                                p.getAs[Long]("pokemonId"))))
      val filtered = maped.filter(alive(_, last_readed))
      val reduced = maped.reduceByKey((p1, p2) => elder(p1, p2))
      val mapresult = reduced.map(p => (pokemons(p._2.pokemonId) ,(p._2.position - new Position(40.4499529, -3.6897580999999993))))
      val result = mapresult.reduceByKey((a, b) => if(a < b) a else b)
      for (p <- result.take(151)) println(p)
    }
}
*/
