/**
 * Created by chrismangold on 12/15/15.
 */
package surgesparkhandler

import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, GeoTiff}
import _root_.geotrellis.spark.io.hadoop.HdfsUtils
import com.typesafe.config.ConfigFactory
import geotrellis.proj4.WebMercator
import geotrellis.raster.ProjectedRaster
import geotrellis.raster._
import geotrellis.engine.RasterSource
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Point, Extent}
import net.liftweb.json._
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path
import scala.math._
import scala.io.Source


object TileUtils {

  val config = ConfigFactory.load()
  val datasource = config.getString("datasource")
  var masterRef = ""

  val filename = "/opt/masterid.cfg"
  for (line <- Source.fromFile(filename).getLines()) {
    masterRef = line
  }
  val masterName = masterRef

  val inPath = "hdfs://" + masterName + ":8020/user/hadoop/tiffs/"

  println("Master Address = " + masterName)

  // Get Tile for processing from Lat Long point
  def GetRasterByPoint(demSrc: String, Lat: Double, Lon: Double): RasterSource = {

    try {

      // TODO Will find file from lookup in catalog index hardcoded for now


      val func: () => Array[Byte] = datasource match {
        case "hdfs" => GetHDFSData
        //  case "S3" => AmazonConnect.downloadFromS3
      }


      val bytesToUse = func()
      // Get SingleBandGeotiff
      val geoTiff = SinglebandGeoTiff(bytesToUse)

      // Get Projected Raster
      val projectedRaster = geoTiff.projectedRaster

      // Get Projected Raster that has been reprojected
      val ProjectedRaster( Raster(tile, extent), crs) = projectedRaster.reproject(WebMercator)

      // Back to a SingleBandGeoTiCC
      val reprojGeoTiff = SinglebandGeoTiff(tile, extent, crs, geoTiff.tags, geoTiff.options)

      RasterSource(reprojGeoTiff, geoTiff.extent)

    } catch {
      case ex: Exception => println(ex.getMessage());
        null
    }

  }

  // Get array of tiles to process from bounding box
  def GetTilesFromBBox(demSrc: String, LLLat: Float, LLLon: Float, URLLat: Float, URLon: Float): RasterSource = {

    try {

      // TODO Will find file from lookup in catalog index hardcoded for now
      // TODO Wiil need to combine return tile from search



      val func: () => Array[Byte] = datasource match {
        case "hdfs" => GetHDFSData
        //  case "S3" => AmazonConnect.downloadFromS3
      }


      val bytesToUse = func()

      //Construct an object with instructions to fetch the raster
      // Returns SingleBandGeoTiff
      val geoTiff = SinglebandGeoTiff(bytesToUse)

      // Returns a ProjectedRaster
      val projectedRaster = geoTiff.projectedRaster

      val ProjectedRaster( Raster(tile, extent), crs) = projectedRaster.reproject(WebMercator)

      // Returns SingleBandGeoTiff
      val reprojGeoTiff = SinglebandGeoTiff(tile, extent, crs, geoTiff.tags, geoTiff.options)

      println("Reprojected new extent: " + extent)

      // Move from tile to rastersource
      RasterSource(reprojGeoTiff, geoTiff.extent)


    } catch {
      case ex: Exception => println(ex.getMessage());
        null
    }

  }

  def GetHDFSPathRoot(): String = {

    val pathStr = "hdfs://" + masterName + ":8020/"
    pathStr
  }

  def WriteHDFSData( inArray : Array[Byte], directoryName:String, fileName: String ): Unit  = {

    val hadoopConf = SparkUtils.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    val resultStr = "hdfs://" + masterName + ":8020/" + directoryName + "/" + fileName
    val resultPath =  new Path( resultStr )
    val os = fs.create( resultPath )
    os.write(inArray)
    fs.close()
    println("Written to HDFS")

  }

  def GetHDFSData( ): Array[Byte] = {

    val fileName = "18STJ5020.tif"
    val filePath =  inPath + fileName
    val fsPath:Path =  new Path(filePath)
    val conf = SparkUtils.hadoopConfiguration
    val rasterAsBytes = HdfsUtils.readBytes( fsPath, conf)
    println( "GetHDFSData: file read" )
    rasterAsBytes
  }

  def GetHDFSData2( inPath: String ): Array[Byte] = {

    val newPath = "/" + inPath
    println( "GetHDFSData2: " + newPath )
    val fsPath:Path =  new Path(newPath)
    val conf = SparkUtils.hadoopConfiguration
    val rasterAsBytes = HdfsUtils.readBytes( fsPath, conf)
    println( "GetHDFSData: file read" )
    rasterAsBytes
  }

  def getListOfRastersBbox(  srcType: String, LLLat:Float, LLLon:Float, URLLat:Float, URLon:Float): Array[String]= {


    val ext = new Extent( LLLon.toDouble, LLLat.toDouble, URLon.toDouble, URLLat.toDouble )

    // Now call SurgeIndex
    val listOfDocuments = SurgeIndexAPI.getDocumentListFromBbox( ext, srcType )

    var uris:Array[String] = Array[String]()

    // So now we have a list of documents
    val json = parse(listOfDocuments)
    val elements = (json).children
    for (urn <- elements) {

      val uri:String = SurgeIndexAPI.getDocumentContent( urn.values.toString, "type", srcType )

      if (uri.toLowerCase() != "empty".toString()) {
        if ( !uri.contains("wgs84") ) {
          uris +:= uri
        }
      }

    }

    uris


  }

  def getListOfRastersPt(  srcType: String, LLLat:Float, LLLon:Float, URLLat:Float, URLon:Float): Array[String]= {

    val ext = new Extent( LLLon.toDouble, LLLat.toDouble, URLon.toDouble, URLLat.toDouble )


    val centerPt:Point = ext.center
    val nePt:Point = ext.northEast
    // http://www.csgnetwork.com/degreelenllavcalc.html WGS84 meeters at 38 degrees
    val radius = nePt.distance( centerPt ) * 110996.45 // Assuming Meters

    println( "Radius in meters = " + radius)

    // Now call SurgeIndex
    val listOfDocuments = SurgeIndexAPI.getDocumentListFromPt( centerPt.y.toString, centerPt.x.toString, radius.toString, "")

    var uris:Array[String] = Array[String]()

    // So now we have a list of documents
    val json = parse(listOfDocuments)
    val elements = (json).children
    for (urn <- elements) {

      val uri:String = SurgeIndexAPI.getDocumentContent( urn.values.toString, "type", "sources" )

      if (uri.toLowerCase() != "empty".toString()) {
        if ( !uri.contains("wgs84") ) {
          uris +:= uri
        }
      }

    }

    uris


  }

  def determineResolutionLatLng( geotiff: SinglebandGeoTiff ) : String = {

    val swPt = geotiff.raster.rasterExtent.extent.southWest
    val nwPt = geotiff.raster.rasterExtent.extent.northWest
    val distx2 = distanceHaversine( nwPt, swPt)

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


