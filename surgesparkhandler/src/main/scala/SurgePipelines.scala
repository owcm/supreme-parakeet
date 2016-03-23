package surgesparkhandler

import java.nio.file.{Paths, Files}

import geotrellis.engine.RasterSource
import geotrellis.engine.op.{local, elevation, focal}
import geotrellis.proj4.{CRS, WebMercator, LatLng}
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{SinglebandGeoTiff, GeoTiff}
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.vector.Extent
import org.apache.spark.SparkContext
import geotrellis.engine.op.elevation.{ElevationRasterSourceMethodExtensions}
import geotrellis.engine.op.local.{LocalRasterSourceMethodExtensions, MultiplyRasterSourceMethods}
import geotrellis.engine.op.focal.{FocalRasterSourceMethodExtensions}

/**
 * Created by chrismangold on 3/1/16.
 */
object SurgePipelines {

  val INCLUDE_LC_HANDLING = 0x1
  val INCLUDE_OBSTRUCTION_HANDLING = 0x2
  val BARE_EARTH_ONLY = 0x4

  def uuid = java.util.UUID.randomUUID.toString

  def Process( spark: SparkContext, jsonCmdString: String ) : Int  =  {

    val cmdOp = SurgeOpParse.getProperty("op")
    var errCode = 0
    if ( cmdOp == "upload") {

      errCode = upload( spark, jsonCmdString )

    } else if (cmdOp == "buildobs" ) {

      errCode = buildobs( spark, jsonCmdString )
    } else if (cmdOp == "buildobslc" ) {

      errCode = buildobsLandCover(spark, jsonCmdString)
    } else if (cmdOp == "buildhover" ) {

      errCode = buildhover(spark, jsonCmdString)
    }
    errCode
  }

  def writeArtifacts( resolution: String, srcType: String, tiffToWrite: SinglebandGeoTiff ) : Unit = {

    var fileId = uuid.toString

    val fileName = fileId + ".tif"

    val directoryName = srcType + "/" + resolution

    val bytesToWrite = SupportUtils.BAConvert( tiffToWrite )
    TileUtils.WriteHDFSData(bytesToWrite, directoryName, fileName)

    val jsonToSend = JSONBuilder.buildDocumentJSON(srcType, fileName, resolution, tiffToWrite.rasterExtent)

    SurgeIndexAPI.postUploadDocument(jsonToSend)

  }


  def buildobsLandCover(sc: SparkContext, opsParms: String): Int = {


    val demmodelURI = SurgeOpParse.getProperty("demmodelURI")
    val landcoverURI = SurgeOpParse.getProperty("landcoverURI")

    var errcode:Int = 0 ;
    println("demmodel: " + demmodelURI)
    println("landcover: " + landcoverURI)

    var bytesToUse: Array[Byte] = null
    var demReprojGeoTiff: SinglebandGeoTiff = null
    var demCRS: CRS = null
    var demreturnCreated = false
    var demRS: RasterSource = null;
    var lcSbGt : SinglebandGeoTiff = null
    var lcProjRS : RasterSource = null;
    var lcReprojGeoTiff: SinglebandGeoTiff = null
    var demExtent : Extent = null
    var landCoverObstructionCreated = false
    var lcCoverObstructionGT : SinglebandGeoTiff = null
    var obstructGoneTiff: SinglebandGeoTiff = null
    var outputResolution = ""

    try {

      try {

        // Load in the selected DEM Tif
        var (bucketName, filePath) = BuildRdd.parseUri(demmodelURI)

        if (filePath.toLowerCase.contains(".tif") && (bucketName != "empty")) {


          if (bucketName == "hdfs") {
            bytesToUse = TileUtils.GetHDFSData2(filePath)
          } else {
            bytesToUse = AmazonConnect.getS3Object(bucketName, filePath)
          }

          val firstDemSBGT = SinglebandGeoTiff(bytesToUse)

          // Project firstrerturn DEM Raster to LatLng
          val firstProjectedRaster = firstDemSBGT.projectedRaster
          val ProjectedRaster( Raster(firstTile, firstExtent), firstCRS) = firstProjectedRaster.reproject(LatLng)

          // Create Geotiff with new projection
          demReprojGeoTiff = SinglebandGeoTiff(firstTile, firstExtent, firstCRS, firstDemSBGT.tags, firstDemSBGT.options)

          outputResolution = TileUtils.determineResolutionLatLng( demReprojGeoTiff )

          demExtent = firstExtent
          demCRS = firstCRS
          demRS = RasterSource(demReprojGeoTiff, firstExtent)
          //firstExtentToUse = firstExtent
          demreturnCreated = true

        }

      } catch {
        case e: Exception =>
          demreturnCreated = false
          throw new IllegalArgumentException("First Return IO exception will not be included in return")
      }

      try {

        // Read in LandCover
        var (bucketName, filePath) = BuildRdd.parseUri(landcoverURI)

        if (filePath.toLowerCase.contains(".tif") && (bucketName != "empty")) {

          if (bucketName == "hdfs") {
            bytesToUse = TileUtils.GetHDFSData2(filePath)
          } else {
            bytesToUse = AmazonConnect.getS3Object(bucketName, filePath)
          }

          lcSbGt = SinglebandGeoTiff(bytesToUse)

          // Project Land Coverto LatLng
          val lcProjectedRaster = lcSbGt.projectedRaster
          val ProjectedRaster( Raster( lcProjTile, lcProjExtent), lcProjCRS) = lcProjectedRaster.reproject(LatLng)
          // Create Geotiff with new projection
          lcReprojGeoTiff = SinglebandGeoTiff( lcProjTile, lcProjExtent, lcProjCRS, lcSbGt.tags, lcSbGt.options )
          lcProjRS = RasterSource( lcReprojGeoTiff, lcProjExtent)

        } else {
          throw new Exception("Land cover layer not supplied");
        }
      } catch {
        case e: Exception =>
          demreturnCreated = false
          throw new IllegalArgumentException("First Return IO exception will not be included in return")
      }

      try {
        // Now build Obstruction Layer using Land Cover and Just first return dem

        // Need to cut landcover down to size of DEM
        val nwPt = demReprojGeoTiff.raster.rasterExtent.extent.northWest
        val sePt = demReprojGeoTiff.raster.rasterExtent.extent.southEast

        // Now Crop LandCover to DEM Tile
        var lcTile = lcReprojGeoTiff.tile
        val (minX, minY) = lcReprojGeoTiff.raster.rasterExtent.mapToGrid(nwPt.x, nwPt.y)
        val (maxX, maxY) = lcReprojGeoTiff.raster.rasterExtent.mapToGrid(sePt.x, sePt.y)
        var lcCroppedTile = lcTile.crop(minX, minY, maxX, maxY)

        val newCols: Int = demReprojGeoTiff.tile.cols
        val newRows: Int = demReprojGeoTiff.tile.rows
        val resampledTile = lcCroppedTile.resample(lcSbGt.extent, newCols, newRows, NearestNeighbor)
        // Finally reclassify land cover areas
        var tileModified: Tile = resampledTile.map({ (col: Int, row: Int, z: Int) =>

          var zz = 0 // Default to no land Area

          // 1	Forest, Deciduous
          // 2	Forest, Evergreen
          // 3  Scrub/Shrub
          // 4  Grassland
          // 5  Barren/Sparsely Vegetated
          // 6  Urban/Built-Up
          // 7	Agriculture, Other
          // 8  Agriculture, Rice
          // 9	Wetland, Permanent Herbaceous
          //10	Wetland, Mangrove
          //11	Water
          //12	Ice/Snow
          //13	Cloud/Cloud Shadow/No Data
          //14	Wetland, Swamp
          //15	Forest, Mixed

          // Based on Fort Story Landcover.  We will take only
          // 4 Grassland
          // 5 Barren/Sparsely Vegetated
          // 7	Agriculture, Other
          // 8  Agriculture, Rice
          if (z == 4 || z == 5 || z == 7 || z == 8) {
            // Set the cell to visible
            zz = 1
          }

          zz

        })

        // Write out new land cover
        lcCoverObstructionGT = GeoTiff( tileModified, demExtent, demCRS )

        // Landcover RasterSource
        val lcRS = RasterSource (lcCoverObstructionGT, demExtent )

        // We have slope an land cover mask out data that is not sutitable
        val obsOutRS = LocalRasterSourceMethodExtensions(demRS).localMultiply( lcRS )

        obstructGoneTiff = GeoTiff(obsOutRS.get, obsOutRS.rasterExtent.get.extent, demCRS)


        landCoverObstructionCreated = true
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException("Landcover Obstruction Layer IO exception. Will not be included in return")
      }

      if (landCoverObstructionCreated ) {
        writeArtifacts(outputResolution, "lcnoobs", obstructGoneTiff)
        println("obstruction written")
      }

    } catch {

      case e: Exception =>
        errcode = 1
        println( "Exception in build obstruction layer, no layers stored" + e.getMessage)
      case e: IllegalArgumentException =>
        errcode = 1
        println( "Exception in build obstruction layer, no layers stored" + e.getMessage)
    }

    errcode
  }

  def buildobs(sc: SparkContext, opsParms: String): Int = {

    val bareearthlocationURI = SurgeOpParse.getProperty("bareearthlocationURI")
    val firstreturnlocationURI = SurgeOpParse.getProperty("firstreturnlocationURI")

    var errcode:Int = 0 ;
    println("bareearthLocationURI: " + bareearthlocationURI)
    println("firstreturnLocationURI: " + firstreturnlocationURI)

    var firstDemRS: RasterSource = null;
    var bareDemRS: RasterSource = null;
    var finalDemCRS: CRS = null
    var bareReprojGeoTiff: SinglebandGeoTiff = null
    var obstructGoneTiff: SinglebandGeoTiff = null
    var firstReprojGeoTiff: SinglebandGeoTiff = null
    var bareearthCreated = false
    var firstreturnCreated = false
    var firstreturnobstructionsincluded = false
    var processLevels = 0x0
    var outputResolution = ""
    var bytesToUse: Array[Byte] = null

    println("Starting....")

    try {

      try {

        // Load in the selected 1st return DEM Tif
        var (bucketName, filePath) = BuildRdd.parseUri(firstreturnlocationURI)

        if (filePath.toLowerCase.contains(".tif") && (bucketName != "empty")) {


          if (bucketName == "hdfs") {
            bytesToUse = TileUtils.GetHDFSData2(filePath)
          } else {
            bytesToUse = AmazonConnect.getS3Object(bucketName, filePath)
          }

          val firstDemSBGT = SinglebandGeoTiff(bytesToUse)

          // Project firstrerturn DEM Raster to LatLng
          val firstProjectedRaster = firstDemSBGT.projectedRaster
          val ProjectedRaster( Raster(firstTile, firstExtent), firstCRS) = firstProjectedRaster.reproject(LatLng)

          // Create Geotiff with new projection
          firstReprojGeoTiff = SinglebandGeoTiff(firstTile, firstExtent, firstCRS, firstDemSBGT.tags, firstDemSBGT.options)

          firstDemRS = RasterSource(firstReprojGeoTiff, firstExtent)

          outputResolution = TileUtils.determineResolutionLatLng( firstReprojGeoTiff )

          //firstExtentToUse = firstExtent
          firstreturnCreated = true

        }

      } catch {
        case e: Exception =>
          firstreturnCreated = false
          throw new IllegalArgumentException("First Return IO exception will not be included in return")
      }

      try {


        // Process First return first
        var (bucketName, filePath) = BuildRdd.parseUri(bareearthlocationURI)

        if (filePath.toLowerCase.contains(".tif") && (bucketName != "empty")) {

          if (bucketName == "hdfs") {
            bytesToUse = TileUtils.GetHDFSData2(filePath)
          } else {
            bytesToUse = AmazonConnect.getS3Object(bucketName, filePath)
          }

          // Load in the selected Bare earth DEM Tif
          val bareDemSBGT = SinglebandGeoTiff(bytesToUse)

          // Project Bare DEM Raster to LatLng
          val bareProjectedRaster = bareDemSBGT.projectedRaster
          val ProjectedRaster( Raster(bareTile, bareExtent), bareCRS) = bareProjectedRaster.reproject(LatLng)

          // Create Geotiff with new projection
          bareReprojGeoTiff = SinglebandGeoTiff(bareTile, bareExtent, bareCRS, bareDemSBGT.tags, bareDemSBGT.options)
          bareDemRS = RasterSource(bareReprojGeoTiff, bareExtent)

          finalDemCRS = bareCRS

          bareearthCreated = true


        }

      } catch {
        case e: Exception =>
          bareearthCreated = false
          println( e.printStackTrace())
          errcode = 1
          throw new IllegalArgumentException("Invalid bare dem file path");
      }


      try {

          // We have bare earth and first return so build obstruction layer
          val obsOutRS = LocalRasterSourceMethodExtensions(bareDemRS).localIf(firstDemRS, { (z1: Double, z2: Double) =>
            z2 > (z1 + 1.0)
          }, NODATA)

          obstructGoneTiff = GeoTiff(obsOutRS.get, obsOutRS.rasterExtent.get.extent, finalDemCRS)

          firstreturnobstructionsincluded = true


      } catch {
        case e: Exception =>
          errcode = 1
          firstreturnobstructionsincluded = false
          throw new IllegalArgumentException("Failed generation obstruction removed tiles")
      }

      // Now if all files exist then write them to HDFS

      if (firstreturnobstructionsincluded && firstreturnCreated && bareearthCreated) {
        writeArtifacts(outputResolution, "bareearth", bareReprojGeoTiff)
        println("bareearth written")
        writeArtifacts(outputResolution, "firstreturn", firstReprojGeoTiff)
        println("firstreturn written")
        writeArtifacts(outputResolution, "noobs", obstructGoneTiff)
        println("obstruction written")
      }

    } catch {

      case e: Exception =>
        errcode = 1
        println( "Exception in build obstruction layer, no layers stored" + e.getMessage)
      case e: IllegalArgumentException =>
        errcode = 1
        println( "Exception in build obstruction layer, no layers stored" + e.getMessage)
    }

    errcode
  }

  def buildhover(sc: SparkContext, opsParms: String): Int = {

    val bareearthlocationURI = SurgeOpParse.getProperty("bareearthlocationURI")
    val firstreturnlocationURI = SurgeOpParse.getProperty("firstreturnlocationURI")

    var errcode:Int = 0 ;
    println("bareearthLocationURI: " + bareearthlocationURI)
    println("firstreturnLocationURI: " + firstreturnlocationURI)

    var firstDemRS: RasterSource = null;
    var bareDemRS: RasterSource = null;
    var finalDemCRS: CRS = null
    var bareReprojGeoTiff: SinglebandGeoTiff = null
    var hoverTiff: SinglebandGeoTiff = null
    var firstReprojGeoTiff: SinglebandGeoTiff = null
    var bareearthCreated = false
    var firstreturnCreated = false
    var layerBuilt = false
    var processLevels = 0x0
    var outputResolution = ""
    var bytesToUse: Array[Byte] = null

    println("Starting Hover Layer Build....")

    try {

      try {

        // Load in the selected 1st return DEM Tif
        var (bucketName, filePath) = BuildRdd.parseUri(firstreturnlocationURI)

        if (filePath.toLowerCase.contains(".tif") && (bucketName != "empty")) {


          if (bucketName == "hdfs") {
            bytesToUse = TileUtils.GetHDFSData2(filePath)
          } else {
            bytesToUse = AmazonConnect.getS3Object(bucketName, filePath)
          }

          val firstDemSBGT = SinglebandGeoTiff(bytesToUse)

          // Project firstrerturn DEM Raster to LatLng
          val firstProjectedRaster = firstDemSBGT.projectedRaster
          val ProjectedRaster( Raster(firstTile, firstExtent), firstCRS) = firstProjectedRaster.reproject(LatLng)

          // Create Geotiff with new projection
          firstReprojGeoTiff = SinglebandGeoTiff(firstTile, firstExtent, firstCRS, firstDemSBGT.tags, firstDemSBGT.options)

          firstDemRS = RasterSource(firstReprojGeoTiff, firstExtent)

          // Get the resolution of the file
          outputResolution = TileUtils.determineResolutionLatLng( firstReprojGeoTiff )

          //firstExtentToUse = firstExtent
          firstreturnCreated = true

        }

      } catch {
        case e: Exception =>
          firstreturnCreated = false
          throw new IllegalArgumentException("First Return IO exception will not be included in return")
      }

      try {


        // Process First bare earth
        var (bucketName, filePath) = BuildRdd.parseUri(bareearthlocationURI)

        if (filePath.toLowerCase.contains(".tif") && (bucketName != "empty")) {

          if (bucketName == "hdfs") {
            bytesToUse = TileUtils.GetHDFSData2(filePath)
          } else {
            bytesToUse = AmazonConnect.getS3Object(bucketName, filePath)
          }

          // Load in the selected Bare earth DEM Tif
          val bareDemSBGT = SinglebandGeoTiff(bytesToUse)

          // Project Bare DEM Raster to LatLng
          val bareProjectedRaster = bareDemSBGT.projectedRaster
          val ProjectedRaster( Raster(bareTile, bareExtent), bareCRS) = bareProjectedRaster.reproject(LatLng)

          // Create Geotiff with new projection
          bareReprojGeoTiff = SinglebandGeoTiff(bareTile, bareExtent, bareCRS, bareDemSBGT.tags, bareDemSBGT.options)
          bareDemRS = RasterSource(bareReprojGeoTiff, bareExtent)

          finalDemCRS = bareCRS

          bareearthCreated = true


        }

      } catch {
        case e: Exception =>
          bareearthCreated = false
          println( e.printStackTrace())
          errcode = 1
          throw new IllegalArgumentException("Invalid bare dem file path");
      }


      try {

        // We have bare earth and first return so build hover layer
        var obsOutRS = LocalRasterSourceMethodExtensions(firstDemRS).localSubtract(bareDemRS)

        obsOutRS = LocalRasterSourceMethodExtensions(obsOutRS).localMapDouble {
          x => if ( x > 1 ) x else 0 }


        hoverTiff = GeoTiff(obsOutRS.get, obsOutRS.rasterExtent.get.extent, finalDemCRS)

        layerBuilt = true


      } catch {
        case e: Exception =>
          errcode = 1
          layerBuilt = false
          throw new IllegalArgumentException("Failed generation obstruction removed tiles")
      }

      // Now if all files exist then write them to HDFS
      if (layerBuilt ) {
        writeArtifacts(outputResolution, "hover", hoverTiff)
        println("obstruction layer written")
      }

    } catch {

      case e: Exception =>
        errcode = 1
        println( "Exception in build hover layer, no hover layer stored" + e.getMessage)
      case e: IllegalArgumentException =>
        errcode = 1
        println( "Exception in build hover layer, no hover stored" + e.getMessage)
    }

    errcode
  }

  def upload(sc: SparkContext, opsParms: String): Int = {

    val srcType = SurgeOpParse.getProperty( "type")
    val locationUri = SurgeOpParse.getProperty( "locationURI")
    var errcode = 0
    var fileId = uuid.toString

    println( "Source Type: " + srcType)
    println( "locationUri: " + locationUri)

    var (bucketName, filePath) = BuildRdd.parseUri( locationUri )

    println( "bucketName: " + bucketName)
    println( "filePath: " + filePath)

    if ( filePath.toLowerCase.contains( ".tif") && ( bucketName != "empty" )) {

      var bytesToUse = Array[Byte]()
      if (bucketName == "hdfs") {
        bytesToUse = TileUtils.GetHDFSData2(filePath)
      } else {
        bytesToUse = AmazonConnect.getS3Object(bucketName, filePath)
      }

      // Construct an object with instructions to fetch the raster
      // Returns SingleBandGeoTiff
      val gtIn = SinglebandGeoTiff(bytesToUse)

      // Returns a ProjectedRaster
      val projectedRaster = gtIn.projectedRaster

      val ProjectedRaster( Raster(tile, extent), crs) = projectedRaster.reproject(LatLng)

      // Returns SingleBandGeoTiff
      val reprojGeoTiff = SinglebandGeoTiff(tile, extent, crs, gtIn.tags, gtIn.options)

      val fileName = fileId + ".tif"
      val resolution = TileUtils.determineResolutionLatLng(reprojGeoTiff )
      val directoryName = srcType + "/" + resolution

      val bytesToWrite = SupportUtils.BAConvert(reprojGeoTiff)
      TileUtils.WriteHDFSData( bytesToWrite, directoryName, fileName)

      val jsonToSend = JSONBuilder.buildDocumentJSON( srcType, fileName, resolution, reprojGeoTiff.rasterExtent)

      SurgeIndexAPI.postUploadDocument( jsonToSend )

    }

    errcode

  }

}
