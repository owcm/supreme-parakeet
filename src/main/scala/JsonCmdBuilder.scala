package tutorial

import net.liftweb.json._
import net.liftweb.json.Extraction._

// https://github.com/lift/lift/tree/master/framework/lift-base/lift-json/
/**
 * Created by chrismangold on 12/17/15.
 */
object JsonCmdBuilder {

  implicit val formats = DefaultFormats

  case class OpGeometry( `type`: String, coordinates: List[List[List[Double]]])
  case class Feature( `type`: String, geometry: OpGeometry, properties: Map[String, String])
  case class Pipeline( `type`: String, properties: Map[String, String] )

  def buildPointList(lon: Double, lat: Double ) : List[List[List[Double]]] = {

    val pt = List(lon, lat)

    val level2 = List( pt)

    val level1 = List(level2)

    level1
  }

  def buildPolygonList( llLon: Double,  llLat: Double, urLon: Double, urLat: Double ) : List[List[List[Double]]] = {

    val llCorner = List(llLon,llLat)
    val lrCorner = List(urLon,llLat)
    val urCorner = List(urLon,urLat)
    val ulCorner = List(llLon,urLat)
    val llCorner2 = List(llLon,llLat)

    val level2 = List ( llCorner, lrCorner, urCorner, ulCorner, llCorner2)

    val level1 = List(level2)

    level1

  }

  def buildOutput( srcType: String, geoType :String, coordinates:  List[List[List[Double]]], properties: Map[String, String] ) : String = {

    val geometry = OpGeometry( geoType, coordinates )
    val outGoing = Feature( srcType, geometry, properties)

    val json = pretty(render(decompose(outGoing)))

    println( json )

    json

  }

  def buildPipelineJson( srcType: String,  properties: Map[String, String] ) : String = {

    val outGoing = Pipeline( srcType, properties)

    val json = pretty(render(decompose(outGoing)))

    println( json )

    json

  }


}
