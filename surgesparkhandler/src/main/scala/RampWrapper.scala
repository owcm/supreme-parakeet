package surgesparkhandler

import com.typesafe.config.ConfigFactory
import geotrellis.raster.{Tile, RasterExtent, IntArrayTile}
import geotrellis.raster.render._

import geotrellis.vector.{ProjectedExtent, Line, Polygon}

import scala.collection.mutable.ListBuffer


/**
 * Created by chrismangold on 1/20/16.
 */
class RampWrapper {

  val intBasedClassifierMap =
    ColorMap(
      Map(
        0.0 -> RGB(0,0,0).int,
        1.0 -> RGB(0,255,0).int
      )
    )

  val defaultClassifierMap = ColorMap(
    Map(
      0.0 -> RGBA(0xffffe5ff).int,
      0.00001 -> RGBA(0xf7fcb9ff).int,
      0.00002 -> RGBA(0xd9f0a3ff).int,
      0.00003 -> RGBA(0xaddd8eff).int,
      0.00004 -> RGBA(0x78c679ff).int,
      0.00005 -> RGBA(0x41ab5dff).int,
      0.00006 -> RGBA(0x238443ff).int,
      0.00007 -> RGBA(0x006837ff).int,
      1.0 -> RGBA(0x004529ff).int
    )
  )


  val artClassifierMap = ColorMap(
    Map(
      3.5 -> RGB(0,255,0).int,
      7.5 -> RGB(63, 255 ,51).int,
      11.5 -> RGB(102,255,102).int,
      15.5 -> RGB(178, 255,102).int,
      19.5 -> RGB(255,255,0).int,
      23.5 -> RGB(255,255,51).int,
      26.5 -> RGB(255,153, 51).int,
      31.5 -> RGB(255,128,0).int,
      35.0 -> RGB(255,51,51).int,
      40.0 ->RGB(255,0,0).int
    )
  )

  val hlzClassifierMap = ColorMap(
    Map(
      0.0 -> RGBA(255,255,255,0).int,
      1.0 -> RGBA(0xFFFF6600).int,
      2.0 -> RGBA(0xFFCC6600).int,
      3.0 -> RGBA(0x00FF0000).int,
      100.0 -> RGBA(0xFFFF667F).int,
      110.0 -> RGBA(0xFFCC667F).int,
      120.0 -> RGBA(0x00FF007F).int
    )
  )

  val tpiClassifierMap = ColorMap(
    Map(
      -1.0 -> RGBA(0xF7DA22FF).int,
      0.45 -> RGBA(0xECBE1DFF).int,
      0.55 ->  RGBA(0xE77124FF).int,
      0.75-> RGBA(0xD54927FF).int,
      0.85-> RGBA(0xCF3A27FF).int
    )
  )

  val aspectClassifierMap = ColorMap(
    Map(
      22.5 -> RGBA(255,0,0,255).int,
      67.5 -> RGBA(231,113,36,255).int,
      112.5 -> RGBA(246,235,20,255).int,
      157.5 -> RGBA(171,208,80,255).int,
      202.5 -> RGBA(57,198,240,255).int,
      247.5 -> RGBA(70,152,211,255).int,
      292.5 -> RGBA(42,46,127,255).int,
      337.5 -> RGBA(165,34,120,255).int,
      360.0 -> RGBA(255,0,0,255).int
    )
  )


  val config = ConfigFactory.load()

  var colorRampToUse: ColorMap = defaultClassifierMap


  def setColorRamp ( newColorRamp: ColorMap ): Unit = {
    colorRampToUse = newColorRamp
  }

  def getColorRamp ( ): ColorMap  = {
    colorRampToUse
  }

  def getColorBreaks (name: String ): ColorMap = {

    name match {

      case "tpi" => {
        defaultClassifierMap
      }
    }
  }


  def test () : Unit = {


    var tileList= new ListBuffer[(Double, RGBA)]()

    val newEntry = (0.00006, RGBA(0x238443ff))

    tileList += newEntry

    tileList.toArray

  }

  def calcHoverRamp( ) : ColorMap = {

    val endVal = 255
    var rampString = ""

    var hoverMap = Map[Int,Int]()

    for ( a <- 0 to endVal) {
      var redShift = a

      hoverMap += ( a ->  RGB(redShift, 0, 0).int )

    }

    val theClassifier = ColorMap( hoverMap )

    theClassifier

  }

  /*
  def calcHoverRampOld( ) : StrictColorClassifier = {

    val endVal = 255
    var rampString = ""

    for ( a <- 0 to endVal) {
      var redShift = a << 24
      redShift = redShift | 0xFF
      rampString = rampString + a.toDouble + ":" + Integer.toHexString(redShift) + ";"
    }
    rampString = rampString.dropRight(1)

    val colorBreak = ColorBreaks.fromStringDouble(rampString).get

    colorBreak

  }
  */

  def getColorRampByOp( name: String ) : ColorRamp = {


      println( "Received in get color:" + name)
      name match {
        case "hillshade" => {
          ColorRamps.ClassificationMutedTerrain
        }
        case "tpi" => {
          ColorRamps.GreenToRedOrange
        }
        case "draw" => {
          ColorRamps.ClassificationMutedTerrain
        }
        case "aspect" => {
          ColorRamps.ClassificationMutedTerrain
        }
        case "viewshed" => {
          ColorRamps.GreenToRedOrange
        }
        case "slope" => {
          ColorRamps.GreenToRedOrange
        }
        case default => {
          ColorRamps.GreenToRedOrange
        }

      }

  }

}
