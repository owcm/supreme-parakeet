/**
 * Created by chrismangold on 2/26/16.
 */
package surgesparkhandler

/**
 * Created by chrismangold on 2/25/16.
 */

import java.io.{DataOutputStream, ByteArrayOutputStream}

import geotrellis.proj4.{WebMercator, CRS}
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, GeoTiff}
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster.{Tile, VectorToRaster, RasterExtent}
import geotrellis.spark._
import geotrellis.vector._
import org.apache.spark.SparkContext
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import geotrellis.vector.{Point, Extent, ProjectedExtent}
import org.apache.spark.rdd._


object SupportUtils {

  val LZCENTER = 3.0
  val LZMIDDLERING = 2.0
  val LZOUTERRING = 1.0
  val LZCENTERBUFFER = 2
  val LZMIDDLEBUFFER = 8
  val LZOUTERBUFFER = 13
  val LZCENTERID = 120.0
  val LZMIDDLEID = 110.0
  val LZOUTERID = 100.0


  def generateSymbolTile( points: ListBuffer[Point], tile: Tile, code: Int,
                          buffer: Double, crs: CRS, re: RasterExtent) : Tile  = {

    var bufferedPoints = points.map(_.buffer(buffer))

    val newTile:Tile = VectorToRaster.rasterize( bufferedPoints, re, code )

    val cleanTiled = newTile.mapDouble { (col: Int, row: Int, z: Double) =>
      var zz = z
      if (z != code) {
        zz = 0
      }

      zz
    }

    cleanTiled

  }


  def generateSymbolRDD( sc: SparkContext, radius: Double, cellRes: Double, rddItem: TileLayerRDD[SpatialKey]): TileLayerRDD[SpatialKey] = {

    val crs = rddItem.metadata.crs

    val newTiles:RDD[(SpatialKey, Tile)] =

      rddItem.asRasters.flatMap {

        case ( sp, raster) =>

          val re = raster.rasterExtent
          val tileList =  mutable.ListBuffer[(SpatialKey,Tile)]()

          if ( raster.tile.cols > 0 ) {

            val tile = raster.tile

            val points = mutable.ListBuffer[Point]()
            tile.foreach { (col: Int, row: Int, z: Int) =>

              if (z == 3.0 ) {
                val (x, y) = re.gridToMap(col, row)
                points += Point(x, y)
              }

            }

            var codedTiled = tile

            if ( points.length > 0 ) {

              val outerTile = generateSymbolTile(points, tile, LZOUTERID.toInt, LZOUTERBUFFER, crs, re)
              val middleTile = generateSymbolTile(points, tile, LZMIDDLEID.toInt, LZMIDDLEBUFFER, crs, re)
              val innerTile = generateSymbolTile(points, tile, LZCENTERID.toInt, LZCENTERBUFFER, crs, re)

              codedTiled = tile.localIf(outerTile, (a: Double, b: Double) => b == LZOUTERID, LZOUTERID)
              codedTiled = codedTiled.localIf(middleTile, (a: Double, b: Double) => b == LZMIDDLEID, LZMIDDLEID)
              codedTiled = codedTiled.localIf(innerTile, (a: Double, b: Double) => b == LZCENTERID, LZCENTERID)
            }

            val newEntry = (sp, codedTiled)

            tileList += newEntry
          }

          tileList

      }

      //newTiles
     val spkeyRDD = TileLayerRDD( newTiles, rddItem.metadata )

     spkeyRDD

  }

  def BAConvert( geoTiff: SinglebandGeoTiff ): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    try {
      val dos = new DataOutputStream(bos)
      try {
        new GeoTiffWriter(geoTiff, dos).write()
        bos.toByteArray
      } finally {
        dos.close
      }
    } finally {
      bos.close
    }
  }

}