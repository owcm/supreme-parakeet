/**
 * Created by chrismangold on 2/16/16.
 */
package surgesparkhandler

import geotrellis.proj4.{WebMercator, LatLng}
import geotrellis.raster.Tile
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.vector.{Point, Extent, ProjectedExtent}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import geotrellis.raster._


import geotrellis.engine._
import geotrellis.engine.op.elevation.{ElevationRasterSourceMethodExtensions}
import geotrellis.engine.op.local.{LocalRasterSourceMethodExtensions, MultiplyRasterSourceMethods}
import geotrellis.engine.op.focal.{FocalRasterSourceMethodExtensions}
import geotrellis.engine._
import scala.collection.mutable.ListBuffer

import java.nio.file.{Files, Paths}

/**
 * Created by chrismangold on 1/8/16.
 */
object BuildRdd {

  val defaultTiffExtensions: Seq[String] = Seq(".tif", ".TIF", ".tiff", ".TIFF")


  def parseUri( uri:String) : ( String, String)  = {

    if (uri.toLowerCase().contains("s3")) {

      // Then get Bucketname
      val rootStr= uri.substring(("s3://".length) )
      println(rootStr)
      val sepIndex = rootStr.indexOf('/')
      val bucketName = rootStr.substring(0, sepIndex  )
      val filePath = rootStr.substring(sepIndex + 1 )

      (bucketName, filePath)

    } else if (uri.toLowerCase().contains("hdfs")) {
      val rootStr= uri.substring(("hdfs://".length) )
      println(rootStr)
      //val sepIndex = rootStr.indexOf('/')
      // val bucketName = rootStr.substring(0, sepIndex  )
      //val filePath = rootStr.substring(sepIndex + 1 )

      ("hdfs",rootStr)
    } else {
      ("empty","empty")
    }

  }

  def tilePrepRoute( implicit sc: SparkContext, fileList:Array[String] ): RDD[ (ProjectedExtent, Tile) ] =  {


    var tileList= new ListBuffer[(ProjectedExtent,Tile)]()


    var fileCount = 0
    var totalTileSize = 0

    for (name <- fileList) {

      println( "URI path is  " + name )
      var (bucketName, filePath) = parseUri(name)

      if ( filePath.toLowerCase.contains( ".tif") && ( bucketName != "empty" )) {

        var bytesToUse = Array[Byte]()
        if ( bucketName == "hdfs" ) {
          println("Reading in HDFS file")
          bytesToUse = TileUtils.GetHDFSData2( filePath )
        } else {
          bytesToUse = AmazonConnect.getS3Object( bucketName, filePath )
        }

        fileCount += 1

        println( "Number of files loaded " + fileCount)

        // Construct an object with instructions to fetch the raster
        // Returns SingleBandGeoTiff
        val gtIn = SinglebandGeoTiff(bytesToUse)

        // Returns a ProjectedRaster
        val projectedRaster = gtIn.projectedRaster

        val ProjectedRaster( Raster(tile, extent), crs) = projectedRaster.reproject(WebMercator)

        // Returns SingleBandGeoTiff
        val reprojGeoTiff = SinglebandGeoTiff(tile, extent, crs, gtIn.tags, gtIn.options)

        // Move from tile to rastersource
        // val theRS = RasterSource( reprojGeoTiff, extent )

        val ext = reprojGeoTiff.raster.extent

        // Write out product to go to RDD
        val lclTile = reprojGeoTiff.tile
        val lclExtent = reprojGeoTiff.extent
        val projExt = new ProjectedExtent( lclExtent, reprojGeoTiff.crs)
        // val gtSlope = GeoTiff(lclTile, lclExtent, crs)

        val newEntry = (projExt, lclTile)


        tileList += newEntry

      }

    }

    println( "Parallelizing files.")

    //val tiles = sc.makeRDD(tileList, 5)
    val tiles = sc.parallelize( tileList, 4 )


    println( " Number of Tests = " + tiles.count())

    tiles

  }




}