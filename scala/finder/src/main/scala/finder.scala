/* Finder.scala */

import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

import org.apache.spark._
import org.apache.spark.SparkContext._


object Common {

    def filter(line: String, locations: List[String], pokemons: List[String]) = {
      line.toLowerCase().matches(s"^.*(${locations.mkString("|")}|${pokemons.mkString("|")}).*")
    }

    def finder(line: String, locations: List[String], pokemons: List[String]) = {
      var locations_result = ListBuffer[String]()
      for(loc <- locations) if (line.toLowerCase().matches(s"^.*($loc).*")) locations_result += lo

      var result = ListBuffer[Tuple2[String, List[String]]]()
      for(pok <- pokemons) if (line.toLowerCase().matches(s"^.*($pok).*")) result += ((pok, locations_result.toList))
      result
    }

    def group(info: Tuple2[String, List[String]]) = {
      var result = ListBuffer[Tuple2[String, Int]]()
      val locations = info._2.toSet
      for(lo <- locations) result += ((lo, info._2.count(_ == lo)))
      (info._1, result.toList)
    }
}

object Finder {

    def main(args: Array[String]) {
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

      // Define the IO file path.
      val inputFile = "data\\data.txt"
      val outputFile = "data\\out"

      var locations = Source.fromFile("data\\loc.txt").getLines().map(_.toLowerCase()).toList
      var pokemons = Source.fromFile("data\\pok.txt").getLines().map(_.toLowerCase()).toList

      // Create our configuration.
      val conf = new SparkConf().setAppName("Finder")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      val input =  sc.textFile(inputFile)
      // Filter to delete some texts.
      val filters = input.filter(Common.filter(_, locations, pokemons))
      // Search objects and locations.
      val finds = filters.flatMap(Common.finder(_, locations, pokemons))
      // Transform into objects and list of locations.
      val counts = finds.map(find => (find._1, find._2)).reduceByKey{case (x, y) => x ++ y}
      // Transform into objects and list of (locations, matches)
      val result = counts.map(Common.group)
      result.cache()

      // Show the information.
      val info = result.collect()
      for (i <- info) println(i._1 + " ==> " + i._2)
      // Save the information back out to a text file, causing evaluation.
      //result.saveAsTextFile(outputFile)
    }
}
