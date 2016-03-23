/**
 * Created by chrismangold on 1/29/16.
 */
package surgesparkhandler


import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiff, SinglebandGeoTiff}
import geotrellis.raster.mapalgebra.focal.{Circle, Square}
import geotrellis.engine.op.local.{LocalRasterSourceMethodExtensions}
import geotrellis.engine.op.focal.{FocalRasterSourceMethodExtensions}
import geotrellis.raster.render.{ColorRamps, Png}
import geotrellis.proj4.{CRS, WebMercator, LatLng}
import geotrellis.raster.viewshed.Viewshed
import geotrellis.spark._
import geotrellis.spark.mapalgebra.focal.{FocalTileLayerRDDMethods}
import geotrellis.raster.render.{ColorRamps, Png}
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import geotrellis.raster.render.ColorRamps
import scala.util.{Failure, Try}
import org.apache.spark.SparkContext

// API
// http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html

object SurgeRDDOps {


  def aspect( sc: SparkContext,rddItem: TileLayerRDD[SpatialKey], opsParms: String): TileLayerRDD[SpatialKey] = {

    val aspectRdd = rddItem.aspect()
    aspectRdd

  }

  def draw( sc: SparkContext,rddItem: TileLayerRDD[SpatialKey], opsParms: String): TileLayerRDD[SpatialKey] = {

    rddItem

  }

  def hover( sc: SparkContext,rddItem: TileLayerRDD[SpatialKey], opsParms: String): TileLayerRDD[SpatialKey] = {


    rddItem

  }

  def hillShade( sc: SparkContext,rddItem: TileLayerRDD[SpatialKey], opsParms: String): TileLayerRDD[SpatialKey] = {

    println("Hillshade generating")
    val hillShadeRdd = rddItem.hillshade(90, 40, 1.0)
    println("Hillshade Done")
    hillShadeRdd
  }


  def hlz( sc: SparkContext,  rddItem: TileLayerRDD[SpatialKey], opsParms: String ): TileLayerRDD[SpatialKey] = {

    SurgeOpParse.parseCmd( opsParms )

    val diameter = SurgeOpParse.getProperty( "diameter").toFloat
    val radius = diameter / 2

    println( "HLZ diameter: " +  radius)

    println( "HLZ 1")
    val cellsize = SurgeOpParse.getPropertyDouble("cellsize", 1.0)
    val slope = SurgeOpParse.getPropertyDouble("slope", 15.0)

    println( "HLZ cellsize: " +  cellsize)
    println( "HLZ slope: " +  slope)

    val meanRDD:TileLayerRDD[SpatialKey] = rddItem.focalMean(Square(cellsize.toInt))

    println( "HLZ 2")
    // http://www.greenbeltconsulting.com/articles/relationships.html
    // Calculated as percent of slope
    val slopeRDD:TileLayerRDD[SpatialKey] = meanRDD.slope(1)

    println( "HLZ 3")
    // Determine cells with less than slope supplied, map as 1 or 0
    val slopeClassificationRDD = slopeRDD.withContext( _.localMapDouble {
      x => if ( x > 0 && x < slope.toDouble) 1.0 else 0.0 })

    println( "HLZ 4")
    val rasterCellResolution =  rddItem.metadata.cellSize.resolution

    // Now use focalSum to get cell values with radius that can support landing
    var neighborSize = radius // rasterCellResolution
    var focalsumRDD = slopeClassificationRDD.focalSum( Circle(neighborSize) )

    println( "HLZ 5")
    var hist =  slopeClassificationRDD.histogram
    val breaks = hist.quantileBreaks(10)
    println( "Number of breaks " + breaks.length )

    if ( breaks.length <= 0 ) {
      throw new Exception("No valid HLZ cells found")
    }

    val maxValue = hist.maxValue().get
    println( "maxValue " + maxValue)

    var topBreak:Int = maxValue
    var outerBreak:Int = maxValue
    var extreameOuter:Int =maxValue

    if (  breaks.length >= 3 ) {
       topBreak = breaks( breaks.length - 1)
       outerBreak = breaks( breaks.length - 2)
       extreameOuter = breaks( breaks.length - 3)
    }

    println( "HLZ 6")
    val focalSumClassificationRDD = focalsumRDD.withContext( _.localMapDouble {
      x => if ( x >= maxValue ) {
        SupportUtils.LZCENTER
      } else if ( x >= outerBreak ) {
        SupportUtils.LZMIDDLERING
      } else if ( x >= extreameOuter ) {
        SupportUtils.LZOUTERRING
      } else {
        0.0
      }
    })

    println( "HLZ 7")
    val rddCRS = focalSumClassificationRDD.metadata.crs
    val combinedRDD = SupportUtils.generateSymbolRDD( sc, radius, rasterCellResolution,
        focalSumClassificationRDD)

    println( "HLZ 8")
    combinedRDD

  }



  //http://gis4geomorphology.com/roughness-topographic-position/
  // 1.) Relative Topographic Position
  def tpi( sc: SparkContext, rddItem: TileLayerRDD[SpatialKey], opsParms: String): TileLayerRDD[SpatialKey] = {

    /*
    val cellsize = SurgeOpParse.getPropertyDouble("cellsize", 2.0)

    val tpiMinRS = FocalRasterRDDMethodExtensions(rddItem).focalMin( Square(cellsize.toInt) )

    val tpiMaxRS = FocalRasterRDDMethodExtensions(rddItem).focalMax( Square(cellsize.toInt) )

    val tpiMeanRS = FocalRasterRDDMethodExtensions(rddItem).focalMean( Square(cellsize.toInt ))

    // Normalize smoothed raster
    val resultNorm =tpiMeanRS.localSubtract(tpiMinRS)
    val resultDivider = tpiMaxRS.localDivide( tpiMinRS )

    val tpiResult = resultNorm.localSubtract( resultDivider )

    tpiResult
    */
    val cellsize = SurgeOpParse.getPropertyDouble("cellsize", 3.0)

    val tpiMinRS = rddItem.focalMin( Square(cellsize.toInt) )

    val tpiMaxRS = rddItem.focalMax( Square(cellsize.toInt) )

    val tpiMeanRS = rddItem.focalMean( Square(cellsize.toInt ))

    // Normalize smoothed raster
    val resultNorm:TileLayerRDD[SpatialKey] = tpiMeanRS.withContext( _.localSubtract(tpiMinRS) )
    val resultDivider:TileLayerRDD[SpatialKey] = tpiMaxRS.withContext( _.localSubtract( tpiMinRS ) )

    val tpiResult:TileLayerRDD[SpatialKey] = resultNorm.withContext(_.localDivide( resultDivider ) )

    tpiResult

  }

  //http://gis4geomorphology.com/roughness-topographic-position/
  // 8.) TRI (Riley)
  def tpiRiley( sc: SparkContext, rddItem: TileLayerRDD[SpatialKey], opsParms: String): TileLayerRDD[SpatialKey] = {

    val cellsize = SurgeOpParse.getPropertyDouble("cellsize", 1.0)

    val tpiMinRS = rddItem.focalMin( Square(cellsize.toInt) )

    val tpiMaxRS = rddItem.focalMax( Square(cellsize.toInt) )

    val squareMinium =  tpiMinRS.withContext(_.localMultiply(tpiMinRS))
    val squareMaxium =  tpiMaxRS.withContext(_.localMultiply(tpiMaxRS))

    val squareDelta = squareMaxium.withContext(_.localSubtract( squareMinium ))
    val squareAbs = squareDelta.withContext(_.localAbs())

    val finalResult = squareAbs.withContext(_.localSqrt())


    finalResult

  }


  def slope(sc: SparkContext, rddItem: TileLayerRDD[SpatialKey], opsParms: String): TileLayerRDD[SpatialKey] = {

    val slopeRdd = rddItem.slope(1.0)

    slopeRdd
  }


  def tileMakeRDD( implicit sc: SparkContext, theTile: Tile, crs:CRS, ext:Extent ): RDD[ (ProjectedExtent, Tile) ] =  {

    val lclExtent = ext
    val projExt = new ProjectedExtent( lclExtent, crs )

    var tileList= new ListBuffer[(ProjectedExtent,Tile)]()
    val newEntry = (projExt, theTile)

    tileList += newEntry

    sc.makeRDD( tileList, 4 )

  }

  def viewShedComposite(sc: SparkContext, rddItem: TileLayerRDD[SpatialKey], opsParms: String): TileLayerRDD[SpatialKey] = {

     var stitchedTile = rddItem.stitch
     var ext = stitchedTile.extent

     var vsTile = Viewshed( stitchedTile, stitchedTile.cols/2, stitchedTile.rows/2 )

     var bytesToWrite:Array[Byte] = vsTile.renderPng().bytes

     TileUtils.WriteHDFSData(bytesToWrite,"test", "largevs.png")

     rddItem

  }

/*
  def viewShed(sc: SparkContext, rddItem: RasterRDD[SpatialKey], opsParms: String): RasterRDD[SpatialKey] = {

    val tgtHgt = SurgeOpParse.getPropertyDouble("tgtHgt", 1.0)
    val orgHgt = SurgeOpParse.getPropertyDouble("orgHgt", 1.0)
    val lon = SurgeOpParse.getLon(0)
    val lat = SurgeOpParse.getLat(0)

    // Reproject to LatLng to to get correct corrdinate conversion
    // Get Projected Raster

    rddItem.foreach( x=> {

      val ext = x._1.asInstanceOf[ProjectedExtent].extent
      val tile = x._2
      val geotiff = GeoTiff(tile, ext, LatLng)

      val projectedRasterWM = geotiff.projectedRaster

      // Get Projected Raster that has been reprojected
      val ProjectedRaster(tileLL, extentLL, crsLL) = projectedRasterWM.reproject(LatLng)

      val rsExtent = RasterExtent(tileLL, extentLL)

      val jsonDesc = Json.RasterExtentToJson(rsExtent)

      println(jsonDesc.toJson())

      // Convert lat/long to x/y grid points
      val xx = rsExtent.mapXToGrid(lon)
      val yy = rsExtent.mapYToGrid(lat)

      // Verify that coordinates are with in range.
      if (xx < 0 || yy < 0 || xx > tile.rows || yy > tile.cols) {

        println( "Skipping Tile")

      } else {

        println( xx + " " + yy )
        val orgVal = tile.getDouble(xx, yy)

        var geoModified: Tile = x._2

        val rows = geoModified.rows
        val cols = geoModified.cols

        var geoModified2: Tile = geoModified.mapDouble { (col: Int, row: Int, z: Double) =>

          var zz = z
          if (col == xx && row == yy) {
            zz = orgVal + orgHgt
          }
          zz
        }
      }

    })

    var cummulativeTile = ApproxViewshedExt(geoModified2, x, y)

    if (tgtHgt > 0) {

      for (cc <- 0 until 20) {

        for (rr <- 0 until 20) {

          var tgtModified: Tile = geoModified2.mapDouble { (col: Int, row: Int, z: Double) =>

            var zz = z
            if (col == cc & row == rr) {
              zz = z + tgtHgt
            }
            zz
          }

          val vsTile = ApproxViewshed(tgtModified, x, y)

          cummulativeTile = cummulativeTile.combineDouble(vsTile) {
            (z1: Double, z2: Double) =>

              var zz = z1

              if (z2 > z1) {
                zz = z2
              }

              zz
          }
        }
      }

    }

    val rs = RasterSource(cummulativeTile, inRasterSource.rasterExtent.get.extent)

    // Setup visible and not visible color scheme
    val visColor = Color.parseColor("0000FF") // Blue
    val invisColor = Color.parseColor("00FF00") // Green

    // http://geotrellis.github.io/scaladocs/0.9/index.html#geotrellis.rest.op.string.ParseColor
    // val paletteColors:Array[Int] = palette.split(",").map(Color.parseColor(_))
    var colors = new Array[Int](2)
    colors(0) = invisColor & 0x00
    colors(1) = visColor

    rs

  }
*/


}




