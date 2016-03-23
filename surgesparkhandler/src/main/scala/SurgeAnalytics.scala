package surgesparkhandler

import com.typesafe.config.ConfigFactory
import geotrellis.proj4.WebMercator
import geotrellis.raster.Tile
import geotrellis.raster.resample.{Bilinear, NearestNeighbor}
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark._
import org.apache.spark.SparkContext
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme,TilerMethods}
import geotrellis.raster.render._
import geotrellis.spark.ingest._
import org.apache.spark.rdd.RDD
import geotrellis.spark.io.file.{FileLayerWriter}
import scala.collection.mutable


/**
 * Created by chrismangold on 3/1/16.
 */
object SurgeAnalytics {

  val cw = new RampWrapper()

  val colorBreaks:ColorMap = cw.defaultClassifierMap
  val artColorBreaks:ColorMap = cw.artClassifierMap
  val tpiColorBreaks: ColorMap = cw.tpiClassifierMap
  val hlzColorbreaks: ColorMap = cw.hlzClassifierMap
  val aspectColorBreaks: ColorMap = cw.aspectClassifierMap
  var hoverColorBreaks: ColorMap = cw.calcHoverRamp()

  val LightToDarkGrey = ColorRamp(
    0xF4FBF6,0xF0F7F2, 0xECF3EE,0xE8EFEA,
    0xE4EBE6, 0xE0E7E2, 0xDCE3DE, 0xD8DFDA,
    0xD5DBD6, 0xD1D7D2, 0xCDD3C3, 0XC9CFCA,
    0xC5CBC6, 0xC1C7C2, 0xBDC3BE, 0xBABFBA,
    0xB6BBB6, 0xB2B7B2, 0xAEB3AE, 0xAAAFAA,
    0xA6ABA6, 0xA2A7A2, 0x9FA39F)

  val rootMetadataPath =  "tile/metadata/"

  def Process( implicit sc : SparkContext, jsonCmdString: String ) : Int = {

    var rsIn: Array[String] = Array[String]()
    var errcode:Int = 0 ;
    var llLat: Float = 0
    var llLon: Float = 0
    var urLat: Float = 0
    var urLon: Float = 0
    var demSrcType: String = "sources"

    try {

      if (SurgeOpParse.geometryType == "Polygon") {

        llLat = SurgeOpParse.getLat(0)
        llLon = SurgeOpParse.getLon(0)
        urLat = SurgeOpParse.getLat(2)
        urLon = SurgeOpParse.getLon(2)

        // Determine the DemSrcType We are going to use
        demSrcType = SurgeOpParse.getFeatureType()

        println("Src is : " + demSrcType)
        rsIn = TileUtils.getListOfRastersBbox(demSrcType, llLat, llLon, urLat, urLon)

        println("Array " + rsIn)

      } else {

        val x = SurgeOpParse.getLon(0)
        val y = SurgeOpParse.getLat(0)

      }

      val cmdOp = SurgeOpParse.getProperty("op")
      val jobId = SurgeOpParse.getProperty("jobid")

      if (cmdOp != "upload") {

        println("Processing Data")

        // Read the geotiff tiles in as an RDD from S3 or HDFS
        val inputTiles = BuildRdd.tilePrepRoute(sc, rsIn)

        println("Processing Set FloatingLayoutScheme")

        val (_, tileLayerMetadata) = TileLayerMetadata.fromRdd(inputTiles, FloatingLayoutScheme(512))

        val tiled: RDD[(SpatialKey, Tile)] = inputTiles.tileToLayout(tileLayerMetadata, Bilinear)

        println("Call operation " + cmdOp )
        val opFunc: (SparkContext, TileLayerRDD[(SpatialKey)], String) => TileLayerRDD[(SpatialKey)] = cmdOp match {

          case "hillshade" => {
            SurgeRDDOps.hillShade
          }
          case "tpi" => {
            SurgeRDDOps.tpi
          }
          case "draw" => {
            SurgeRDDOps.draw
          }
          case "artslope" => {
            SurgeRDDOps.slope
          }
          case "slope" => {
            SurgeRDDOps.slope
          }
          case "hlz" => {
            SurgeRDDOps.hlz
          }
          case "aspect" => {
            SurgeRDDOps.aspect
          }
          case "hover" => {
            SurgeRDDOps.hover
          }
          /*
        case "viewshed" => {
          SurgeRDDOps.viewShed
        } */
          case default => null
          //  case "S3" => AmazonConnect.downloadFromS3
        }

        if (opFunc != null) {

          println("Set layout schema")
          // We'll be tiling the images using a zoomed layout scheme
          // in the web mercator format (which fits the slippy map tile specification).
          // We'll be creating 256 x 256 tiles.
          val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

          println("Set reprojected")
          // We need to reproject the tiles to WebMercator
          val (zoom, reprojected) = TileLayerRDD(tiled, tileLayerMetadata).reproject(WebMercator, layoutScheme, Bilinear)

          println("Call operation")
          //val spkeyRDD = tiled.toRasterRDD(rasterMetaData)
          val resultRDD = opFunc(sc, reprojected, jsonCmdString)

          // Create the writer that we will use to store the tiles in the local catalog.
          val writer = PNGClassWriter( cmdOp, jobId )
          //[SpatialKey, Tile, rasterMetaData](outputPath, ZCurveKeyIndexMethod)

          println("Pyramiding")
          // Pyramiding up the zoom levels, write our tiles out to the local file system.
          val writeOp =
            Pyramid.upLevels(reprojected, layoutScheme, zoom) {  (rdd, z) =>

              val operation = writer.operation
              val jobId = writer.jobId

              println(" Pyramiding operation " + operation)
              val newTiles:RDD[(SpatialKey, Array[Byte])] =

                rdd.asRasters.flatMap {

                  case ( sp, raster) =>

                    val re = raster.rasterExtent
                    val tileList =  mutable.ListBuffer[(SpatialKey, Array[Byte])]()

                    var pngBytes:Array[Byte] =raster._1.renderPng(colorBreaks).bytes

                    try {

                      if (operation == "artslope") {
                        val testPng = raster._1.renderPng(artColorBreaks)
                        pngBytes = testPng.bytes
                      } else if (operation == "tpi") {
                        pngBytes = raster._1.renderPng(tpiColorBreaks).bytes
                      } else if (operation == "aspect") {
                        pngBytes = raster._1.renderPng(aspectColorBreaks).bytes
                      } else if (operation == "hlz") {
                        pngBytes = raster._1.renderPng(hlzColorbreaks).bytes
                      } else if (operation == "hover") {
                        pngBytes = raster._1.renderPng(hoverColorBreaks).bytes
                      }
                      else {
                        val colorRampToUse = cw.getColorRampByOp(operation )
                        val result = colorRampToUse.toColorMap(raster._1.histogram)
                        pngBytes = raster._1.renderPng(result).bytes
                      }

                      val newEntry = (sp, pngBytes)
                      tileList += newEntry
                    } catch {
                      case e: Exception => println( "Error on reading tile " + e.getMessage)
                    }
                    tileList

                }
              writer.write( z, newTiles, tileLayerMetadata,jobId)

            }

            println("Using " + jobId)
            // Write out Job Result
            val res = tileLayerMetadata.cellSize.resolution.toString
            val output = JSONBuilder.buildJobJSON(jobId, cmdOp, res, llLat, llLon, urLat, urLon, "Success")
            val docPath = rootMetadataPath + jobId
            SurgeIndexAPI.putJobDocument(output, docPath)


          }
        }

      } catch {

        case e: Exception => println( "SurgeAnalytics - exception no Analytic " + e.getMessage)
        errcode = 1
      }

      errcode
  }


}
