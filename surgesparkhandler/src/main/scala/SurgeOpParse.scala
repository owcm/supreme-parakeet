/**
 * Created by chrismangold on 12/15/15.
 */
package surgesparkhandler

import _root_.net.liftweb.json.DefaultFormats
import _root_.net.liftweb.json._
import net.liftweb.json._


object SurgeOpParse {

  implicit val formats = DefaultFormats
  case class OpProperties( opValue: String )
  case class OpGeometry( `type`: String, coordinates: List[List[List[Float]]])
  case class Feature( `type`: String, geometry: OpGeometry, properties: Map[String, String])
  case class Pipeline( `type`: String, properties: Map[String, String] )


  // The Parsed Result

  var propsMap : Map[String, String ] =  _
  var geometryType : String = _
  var featureType : String = _
  var pointsList : List[List[Float]] = _
  var parsed = false
  def parseCmd ( jsonCmd: String ): Unit = {

    val jsonObject = parse( jsonCmd )


    try {

      // Test if Feature
      val pr = jsonObject.extract[Feature]

      // Create Properties Mao
      val props = pr.properties
      propsMap = props.toMap


      // Make Pointer to Feature Points
      pointsList = pr.geometry.coordinates(0)

      // Get Feature and Geometry Points
      geometryType = pr.geometry.`type`
      featureType = pr.`type`

      parsed = true

    } catch {
      case e: Exception => println( " geometryType Not Found")
    }

    if (  parsed == false ) {

      // Do Pipelin Parsing
      val pr = jsonObject.extract[Pipeline]

      // Create Properties Mao
      val props = pr.properties
      propsMap = props.toMap

    }

  }

  def getFeatureType( ) : String = {

    featureType

  }

  def getGeometryType( ) : String = {

    geometryType

  }

  def getPropertyInt( key : String, default: Int ) : Int = {

    var value = default
    try {
      value = propsMap(key).toInt
    } catch {
      case e: NoSuchElementException => println( "Not Found")
    }

    value

  }

  def getPropertyDouble( key : String, default: Double ) : Double = {

    var value = default
    try {
      value = propsMap(key).toDouble
    } catch {
      case e: NoSuchElementException => println( "Not Found")
    }

    value

  }

  def getProperty( key : String ) : String = {

    var value = ""
    try {
      value = propsMap(key)
    } catch {
      case e: NoSuchElementException => println( "Not Found")
    }

    value

  }

  def getLon( corner : Int ) : Float = {

    val pointList = pointsList(corner)
    pointList(0)

  }

  def getLat( corner : Int ) : Float = {

    val pointList = pointsList(corner)
    pointList(1)

  }

}
