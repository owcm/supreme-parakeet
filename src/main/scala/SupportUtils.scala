package tutorial

/**
 * Created by chrismangold on 2/25/16.
 */

import geotrellis.engine.RasterSource
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, GeoTiff}
import geotrellis.raster.{Raster, VectorToRaster, Tile, RasterExtent}
import geotrellis.vector._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math._

object SupportUtils {


  def findMaxHLZDiameter(s1: HLZHelicopterparms, s2: HLZHelicopterparms): HLZHelicopterparms = if (s1.diameter.toFloat >
    s2.diameter.toFloat) s1
  else s2

  def sumHLZDiameter(s1: HLZHelicopterparms, s2: HLZHelicopterparms): HLZHelicopterparms = if (s1.diameter.toFloat >
    s2.diameter.toFloat) s1
  else s2


  def calcMaxAreaDiameter(helicopters: List[HLZHelicopterparms], distanceBuffer: Int): Float = {

    var totalDistance: Float = 0

    helicopters.foreach(f => {
      totalDistance += (f.diameter.toFloat + distanceBuffer)
    })

    totalDistance

  }

  def calcHLZParameters(hlzRequest: HLZPost, jobId: String, demSrcType: String): String = {

    val extent = hlzRequest.extent
    val coordinates = JsonCmdBuilder.buildPolygonList(extent(0),
      hlzRequest.extent(1),
      hlzRequest.extent(2),
      hlzRequest.extent(3))

    val helicopterSeq = hlzRequest.helicopters

    val day = true
    var distanceBuffer = 35 // 35 Meters
    if (!day) {
      distanceBuffer = 50
    }


    val biggestHelicopter = helicopterSeq.reduceLeft(findMaxHLZDiameter)

    val maxDiameter = calcMaxAreaDiameter(helicopterSeq, distanceBuffer)

    printf("Total single landing zone area is diameter:" + maxDiameter)

    val properties = Map("op" -> "hlz",
      "jobid" -> jobId,
      "optype" -> "analytic",
      "approach" -> biggestHelicopter.approach,
      "degrees" -> biggestHelicopter.degrees,
      "diameter" -> "25", // Hard 25 meters
      "dimension" -> biggestHelicopter.dimension,
      "obstacleRatio" -> biggestHelicopter.obstacleRatio,
      "distanceBuffer" -> distanceBuffer.toString,
      "spacing" -> "true",
      "slope" -> "15.0"
    )

    JsonCmdBuilder.buildOutput(demSrcType, "Polygon", coordinates, properties)


  }

  def generateSymbolTile(points: ListBuffer[Point], re: RasterExtent, code: Int, buffer: Double, crs: CRS): RasterSource = {

    var bufferedPoints = points.map(_.buffer(buffer))

    val newTile: Tile = VectorToRaster.rasterize(bufferedPoints, re, code)

    var tileModified: Tile = newTile.mapDouble { (col: Int, row: Int, z: Double) =>
      var zz = z
      if (z != code) {
        zz = 0
      }
      zz
    }

    val newTiff = GeoTiff(tileModified, re.extent, crs)
    val newRS = RasterSource(newTiff, re.extent)

    newRS
  }


  def determineResolutionWebMercator( geotiff: SinglebandGeoTiff ) : String = {

    val swPt = geotiff.raster.rasterExtent.extent.southWest
    val nwPt = geotiff.raster.rasterExtent.extent.northWest
    val distx2 = SupportUtils.distanceHaversine( nwPt, swPt)

    var result = 0.0
    var resultInt = 0
    var resultStr = ""
    val distance = distanceHaversine( nwPt, swPt )
    val ans = distance / geotiff.tile.rows

    println( distance + " " + ans)
    if ( ans >= 1.0 ) {
      if ( ans >= 100.0 )
         result = ans % 100.0
       else if (ans >= 10.0 )
         result = ans % 10.0
       else
         result = ans % 1.0
      result = ans - result
      resultInt = result.toInt
      resultStr = resultInt + "M"
    } else {
       result = ans % 0.1
       result = ans - result
       resultInt = (result * 100).toInt
       resultStr = resultInt + "cm"
    }

    resultStr
  }

  def distanceHaversine( point1: Point, point2 : Point) : Double = {

    //http://stackoverflow.com/questions/4102520/how-to-transform-a-distance-from-degrees-to-metres
    println( "Point 1 Lon:"  + point1.x)
    println( "Point 1 Lat:"  + point1.y)
    println( "Point 2 Lon:"  + point2.x)
    println( "Point 2 Lat:"  + point2.y)

    val R = 6371; // km
    val deltaLat = (point2.y - point1.y)
    val deltaLatRadians = toRadians(deltaLat)

    val deltaLon = (point2.x - point1.x)
    val deltaLonRadians = toRadians(deltaLon)


    val a = sin(deltaLatRadians / 2) * sin(deltaLatRadians / 2) +
      cos( toRadians( point1.y) ) * cos( toRadians( point2.y )) *
        sin(deltaLonRadians / 2) * sin( deltaLonRadians / 2)

    var c = 2 * atan2( sqrt(a), sqrt(1 - a)) ;
    var d = (R * c) * 1000;

    d

  }

}


