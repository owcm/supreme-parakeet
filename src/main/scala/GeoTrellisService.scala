package tutorial

import java.io.{FileOutputStream, File}

import com.typesafe.config.ConfigFactory
import geotrellis.raster.mapalgebra.focal.{Circle, Square}
import geotrellis.spark.{SpatialKey}
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import spray.routing.{ExceptionHandler, HttpService}
import spray.http.{MultipartFormData, MediaTypes}
import spray.http.StatusCodes.InternalServerError
import spray.util.LoggingContext
import java.nio.file.{Files, Paths}
import geotrellis.engine._
import geotrellis.engine.op.elevation.{ElevationRasterSourceMethodExtensions}
import geotrellis.engine.op.local.{LocalRasterSourceMethodExtensions, MultiplyRasterSourceMethods}
import geotrellis.engine.op.focal.{FocalRasterSourceMethodExtensions}
import geotrellis.engine.stats.StatsRasterSourceMethodExtensions
import geotrellis.raster.render._
import geotrellis.raster.io.geotiff._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.proj4.CRS
import geotrellis.proj4.LatLng
import geotrellis.proj4.WebMercator
import JsonCmdParse._
import scala.collection.mutable
import geotrellis.vector._
import geotrellis.shapefile.ShapeFileReader

case class OpResponse(jobid: String)
case class VersionResponse( version: String)
case class PingResponse( status: String)
case class UpResponse( status: String)

case class BuildBaseDemResponse ( result: String, bareearthstored: String, firstreturnstored: String, landcoverincluded: String, firstreturnincluded: String,  obstructionstored: String )


trait GeoTrellisService extends HttpService {

  implicit val formats = DefaultFormats
  val INCLUDE_LC_HANDLING = 0x1
  val INCLUDE_OBSTRUCTION_HANDLING = 0x2
  val BARE_EARTH_ONLY = 0x4

  def rootRoute = pingRoute ~ upRoute ~ ingestRoute ~tilePrepRoute ~indexRoute ~gettile ~verRoute ~apiRoute ~buildObstruction ~testRoute

  def uuid = java.util.UUID.randomUUID.toString

  /**
   * http://localhost:8080/ping
   */
  def pingRoute = path("ping") {

    get {
      respondWithMediaType(MediaTypes.`application/json`) {
        complete {

          val op = PingResponse("pong")

          write(op)

        }
      }
    }
  }

  /**
   * http://localhost:8080/ping
   *
   * Simple health check
   */
  def upRoute = path("up") {

    get {

      respondWithMediaType(MediaTypes.`application/json`) {
        complete {

          val op = UpResponse("up")

          write(op)

        }
      }
    }
  }

  /**
   * http://localhost:8080/version
   *
   * Return simple version number
   *
   */
  def verRoute = path("version") {

    get {

      respondWithMediaType(MediaTypes.`application/json`) {
        complete {

          val config = ConfigFactory.load()
          val versionNumber = config.getString("version-name")
          println(versionNumber)

          val op = VersionResponse(versionNumber)

          write(op)
        }
      }
    }
  }

  // Put operations
  // http://localhost:8080/api/hlz
  // http://localhost:8080/api/hlzlc
  // http://localhost:8080/api/hillshade
  // http://localhost:8080/api/aspect
  // http://localhost:8080/api/slope
  // http://localhost:8080/api/viewshed
  // http://localhost:8080/api/artslope
  // http://localhost:8080/api/upload


  // Get Operations
  // http://localhost:8080/api/status/{jobid}
  // http://localhost:8080/api/cancel/{jobid}
  def apiRoute =  {

    post {
      pathPrefix("api") {

        path("uploadfile") {

          entity(as[Array[Byte]]) { value =>
            val byteWriter = new FileOutputStream(new File("/Users/chrismangold/develop/preparedata/test.ext"))
            byteWriter.write(value);
            byteWriter.close()
            respondWithMediaType(MediaTypes.`application/json`) {
              complete {
                val jobid = uuid.toString
                val op = OpResponse(jobid)

                write(op)
              }
            }
          }

        }~
        path("hlz") {

          entity(as[HLZPost]) { hlzrequest =>

            respondWithMediaType(MediaTypes.`application/json`) {
              complete {

                val jobid = uuid.toString
                val cmd = SupportUtils.calcHLZParameters(hlzrequest, jobid, "noobs")

                SparkSubmit.submitJob(jobid, "HLZ", cmd)

                val op = OpResponse(jobid)

                write(op)

              }
            }
          }
        } ~
          path("hlzlc") {

            entity(as[HLZPost]) { hlzrequest =>

              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val cmd = SupportUtils.calcHLZParameters(hlzrequest, jobid, "lcnoobs")

                  SparkSubmit.submitJob(jobid, "HLZLC", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }
            }
          } ~
          path("viewshedtest") {

            entity(as[GenericOpPost]) { oprequest =>
              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val coordinates = JsonCmdBuilder.buildPolygonList(oprequest.extent(0),
                    oprequest.extent(1),
                    oprequest.extent(2),
                    oprequest.extent(3))

                  val properties = Map("op" -> "vscomp", "jobid" -> jobid,  "optype" -> "analytic")
                  val cmd = JsonCmdBuilder.buildOutput("sources", "Polygon", coordinates, properties)

                  SparkSubmit.submitJob(jobid, "VIEWSHEDTEST", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }

            }
          } ~
          path("hillshade") {

            entity(as[GenericOpPost]) { oprequest =>
              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val coordinates = JsonCmdBuilder.buildPolygonList(oprequest.extent(0),
                    oprequest.extent(1),
                    oprequest.extent(2),
                    oprequest.extent(3))

                  val properties = Map("op" -> "hillshade", "jobid" -> jobid,  "optype" -> "analytic")
                  val cmd = JsonCmdBuilder.buildOutput("sources", "Polygon", coordinates, properties)

                  SparkSubmit.submitJob(jobid, "HILLSHADE", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }

            }
          } ~
          path("hover") {

            entity(as[GenericOpPost]) { oprequest =>
              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val coordinates = JsonCmdBuilder.buildPolygonList(oprequest.extent(0),
                    oprequest.extent(1),
                    oprequest.extent(2),
                    oprequest.extent(3))

                  val properties = Map("op" -> "hover", "jobid" -> jobid,  "optype" -> "analytic")
                  val cmd = JsonCmdBuilder.buildOutput("hover", "Polygon", coordinates, properties)

                  SparkSubmit.submitJob(jobid, "HOVER", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }

            }
          } ~
          path("draw") {
            entity(as[GenericOpPost]) { oprequest =>

              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val coordinates = JsonCmdBuilder.buildPolygonList(oprequest.extent(0),
                    oprequest.extent(1),
                    oprequest.extent(2),
                    oprequest.extent(3))

                  val properties = Map("op" -> "draw", "jobid" -> jobid,  "optype" -> "analytic")
                  val cmd = JsonCmdBuilder.buildOutput("sources", "Polygon", coordinates, properties)

                  SparkSubmit.submitJob(jobid, "draw", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }
            }
          } ~
          path("tpi") {
            // Referance http://gis4geomorphology.com/roughness-topographic-position/
            entity(as[TPIOpPost]) { oprequest =>

              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val coordinates = JsonCmdBuilder.buildPolygonList(oprequest.extent(0),
                    oprequest.extent(1),
                    oprequest.extent(2),
                    oprequest.extent(3))

                  val properties = Map("op" -> "tpi", "cellsize" -> oprequest.parms.cellsize, "jobid" -> jobid,  "optype" -> "analytic")
                  val cmd = JsonCmdBuilder.buildOutput("sources", "Polygon", coordinates, properties)

                  SparkSubmit.submitJob(jobid, "TPI", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }

            }
          } ~
          path("aspect") {

            entity(as[GenericOpPost]) { oprequest =>
              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val coordinates = JsonCmdBuilder.buildPolygonList(oprequest.extent(0),
                    oprequest.extent(1),
                    oprequest.extent(2),
                    oprequest.extent(3))
                  val properties = Map("op" -> "aspect", "jobid" -> jobid,  "optype" -> "analytic")
                  val cmd = JsonCmdBuilder.buildOutput("sources", "Polygon", coordinates, properties)

                  SparkSubmit.submitJob(jobid, "ASPECT", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }
            }
          } ~
          path("slope") {
            entity(as[GenericOpPost]) { oprequest =>
              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val coordinates = JsonCmdBuilder.buildPolygonList(oprequest.extent(0),
                    oprequest.extent(1),
                    oprequest.extent(2),
                    oprequest.extent(3))
                  val properties = Map("op" -> "slope", "jobid" -> jobid, "optype" -> "analytic")
                  val cmd = JsonCmdBuilder.buildOutput("sources", "Polygon", coordinates, properties)

                  SparkSubmit.submitJob(jobid, "SLOPE", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }
            }
          } ~
          path("artslope") {
            entity(as[GenericOpPost]) { oprequest =>

              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val coordinates = JsonCmdBuilder.buildPolygonList(oprequest.extent(0),
                    oprequest.extent(1),
                    oprequest.extent(2),
                    oprequest.extent(3))
                  val properties = Map("op" -> "artslope",  "optype" -> "analytic", "jobid" -> jobid)
                  val cmd = JsonCmdBuilder.buildOutput("sources", "Polygon", coordinates, properties)

                  SparkSubmit.submitJob(jobid, "ARTSLOPE", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }
            }
          } ~
          path("viewshed") {
            entity(as[ViewShedPost]) { oprequest =>

              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val jobid = uuid.toString
                  val coordinates = JsonCmdBuilder.buildPointList(oprequest.pt(0),
                    oprequest.pt(1))
                  val properties = Map("op" -> "viewshed",  "optype" -> "analytic",
                    "tgtHgt" -> oprequest.parms.tgtHgt, "orgHgt" -> oprequest.parms.orgHgt, "jobid" -> jobid)
                  val cmd = JsonCmdBuilder.buildOutput("sources", "Polygon", coordinates, properties)

                  SparkSubmit.submitJob(jobid, "Viewshed", cmd)

                  val op = OpResponse(jobid).toString

                  write(op)

                }
              }

            }
          } ~
          path("upload") {
            entity(as[UploadPost]) { uploadrequest =>

                respondWithMediaType(MediaTypes.`application/json`) {
                  complete {

                    val uploadType = uploadrequest.`type`
                    val locationURI = uploadrequest.location

                    val properties = Map("op" -> "upload",  "optype" -> "pipeline", "type" -> uploadType, "locationURI" -> uploadrequest.location)
                    val cmd = JsonCmdBuilder.buildPipelineJson("sources", properties)

                    val jobid = uuid.toString

                    SparkSubmit.submitJob(jobid, "Upload", cmd)

                    val op = OpResponse(jobid)

                    write(op)

                  }
                }
            }
          } ~
          path("buildobs") {
            entity(as[BuildObsPost]) { uploadrequest =>

              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val bareearthLocationURI = uploadrequest.bareearthlocation
                  val firstreturnLocationURI = uploadrequest.firstreturnlocation

                  val properties = Map("op" -> "buildobs",  "optype" -> "pipeline", "bareearthlocationURI" -> bareearthLocationURI,
                    "firstreturnlocationURI" -> firstreturnLocationURI)

                  val cmd = JsonCmdBuilder.buildPipelineJson("sources", properties)

                  val jobid = uuid.toString

                  SparkSubmit.submitJob(jobid, "Buildops", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }
            }
          }~
          path("buildhover") {
            entity(as[BuildHoverPost]) { uploadrequest =>

              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val bareearthLocationURI = uploadrequest.bareearthlocation
                  val firstreturnLocationURI = uploadrequest.firstreturnlocation

                  val properties = Map("op" -> "buildhover",  "optype" -> "pipeline", "bareearthlocationURI" -> bareearthLocationURI,
                    "firstreturnlocationURI" -> firstreturnLocationURI)

                  val cmd = JsonCmdBuilder.buildPipelineJson("sources", properties)

                  val jobid = uuid.toString

                  SparkSubmit.submitJob(jobid, "Buildhover", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }
            }
          }~
          path("buildobslc") {
            entity(as[BuildObsLCPost]) { uploadrequest =>

              respondWithMediaType(MediaTypes.`application/json`) {
                complete {

                  val demmodelURI = uploadrequest.srcdem
                  val landcoverURI = uploadrequest.srclandcover

                  val properties = Map("op" -> "buildobslc",  "optype" -> "pipeline", "demmodelURI" -> demmodelURI,
                    "landcoverURI" -> landcoverURI)

                  val cmd = JsonCmdBuilder.buildPipelineJson("sources", properties)

                  val jobid = uuid.toString

                  SparkSubmit.submitJob(jobid, "Buildopslc", cmd)

                  val op = OpResponse(jobid)

                  write(op)

                }
              }
            }
          }
        }
      }~
    get {
      pathPrefix("api") {
        path( "status" / RestPath) { jobid =>

          println("Status for:" + jobid)
          val jobIdStr = jobid.toString
          respondWithMediaType(MediaTypes.`application/json`) {
            complete {

              val op = JobStatus( jobIdStr, SparkJobTracker.getJobStatus(jobIdStr) )

              write(op)
            }
          }
        } ~
          path( "cancel" / RestPath) { jobid =>

            println( "Cancel job: " +  jobid)
            val jobIdStr = jobid.toString
            respondWithMediaType(MediaTypes.`application/json`) {
              complete {

                val op = JobStatus( jobIdStr, SparkJobTracker.killSparkJob(jobIdStr) )

                write(op)
              }
            }
          }

      }
    }
  }


  // http://alvinalexander.com/scala/scala-rest-client-apache-httpclient-restful-clients
  def indexRoute = path("getdocs") {

    get {
      parameters('lat ? "65.9998611", 'lon ? "35.0001389", 'meters ? "500") {
        (lat, lon, meters) => {

          complete {

            val testFilter = ""
            val content = SurgeIndexAPI.getDocumentListFromPt(lat, lon, meters, testFilter)

            // So now we have a list of documents
            val json = parse(content)
            val elements = (json).children
            for (urn <- elements) {

              val content = SurgeIndexAPI.getDocumentContent( urn.values.toString, "type", "sources" )

            }

            "Done"

          }
        }
      }
    }
  }

  def testRoute = {

    get {
      pathPrefix("test") {

        path("roadstest") {

          get {

            respondWithMediaType(MediaTypes.`application/json`) {
              complete {

                // Load The LandCover Layer
                var lcName = "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/n36w077_fme_m.tif"

                val bytesToUse = Files.readAllBytes(Paths.get(lcName.toString))
                val lcSbGt = SinglebandGeoTiff(bytesToUse)

                val roadFeatures = ShapeFileReader.readMultiLineFeatures("/Users/chrismangold/develop/preparedata/shapefiles/FortStoryRoads.shp")

                println( "Number of Features:" + roadFeatures.size )
                println( "Number of Features:" + roadFeatures.length )

                //Reproject.
                roadFeatures.foreach( f => {

                  val mlf: MultiLineFeature = f.
                  val reprojected: MultiLineFeature[f]] = mlf.reproject(oldCRS, newCRS)

                })

                  var test = f.geom
                  var myLine = test.lines(0)
                  myLine.rep

                  f.data.foreach( f=> {

                  })

                })

                val roadColorbreaks =
                 ColorBreaks.fromStringDouble(ConfigFactory.load().getString("road.colorbreaks")).get

                val tile = ArrayTile.empty(lcSbGt.raster.cellType, lcSbGt.raster.rasterExtent.cols, lcSbGt.raster.rasterExtent.rows)

                roadFeatures.foreach( f=> {

                   Rasterizer.foreachCellByMultiLineString( f.geom, lcSbGt.raster.rasterExtent) { (col: Int, row: Int) =>
                     f.geom.
                     tile.set(col,row,20)
                   }

                })

                tile.renderPng(roadColorbreaks).write("/Users/chrismangold/develop/preparedata/rasterized/test.png")

                val op = UpResponse("Finished")

                write(op)

              }
            }
          }
        }
      }
    }
  }
  /**
   * http://localhost:8080/
   *
   * Ingest - create Slope, Aspect, TPI, and Hillshade
   */
  def ingestRoute =  {

    get {
      pathPrefix("ingest") {

        path("slope") {
          parameters('src) {
            (src) => {

              complete {

                val sourcePath = src + "/sources"
                var fileList = AmazonConnect.getObjectsInBucket(sourcePath)

                for (name <- fileList) {

                  println(name._2)
                  if ( name._2.toLowerCase.contains( ".tif") ) {

                    val bytesToUse = AmazonConnect.getS3Object(name._2)

                    // Construct an object with instructions to fetch the raster
                    // Returns SingleBandGeoTiff
                    val gtIn = SinglebandGeoTiff(bytesToUse)

                    // Move from tile to rastersource
                    // Maintain Projections
                    val theRS = RasterSource(gtIn, gtIn.extent)

                    // Generate Slope
                    val slopeRS = ElevationRasterSourceMethodExtensions(theRS).slope(0.00000898)

                    // Write out slope product
                    val lclTile = slopeRS.get
                    val lclExtent = slopeRS.rasterExtent.get.extent
                    val gtOut = GeoTiff(lclTile, lclExtent, gtIn.crs)

                    // Build file paths for S3
                    var fileRootindex = name._2.lastIndexOf("/");
                    var fileName = name._2.substring(fileRootindex + 1)

                    // Use when writing local
                    gtOut.write( "/Users/chrismangold/testSlopes/" + fileName )



                  }
                }
                "Done"
              }
            }
          }
        } ~
          path("hlzlcl") {
            parameters('src) {
              (src) => {

                complete {

                  var slopeToUse:SinglebandGeoTiff = null
                  var bytesToUse:Array[Byte] = null
                  var lcSbGt:SinglebandGeoTiff = null
                  var lcProjRS:RasterSource = null
                  var lcProjTile = null
                  var lcReprojGeoTiff:SinglebandGeoTiff = null

                  // Load The LandCover Layer
                  var lcName = "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/n36w077_fme_m.tif"

                  if (lcName.length > 0) {
                    bytesToUse = Files.readAllBytes(Paths.get(lcName.toString))
                    lcSbGt = SinglebandGeoTiff(bytesToUse)

                    // Project Land Coverto WebMercator
                    val lcProjectedRaster = lcSbGt.projectedRaster

                    val ProjectedRaster(Raster(lcProjTile, lcProjExtent), crs) = lcProjectedRaster.reproject(WebMercator)

                    // Create Geotiff with new projection
                    lcReprojGeoTiff = SinglebandGeoTiff( lcProjTile, lcProjExtent, crs )
                    lcProjRS = RasterSource( lcReprojGeoTiff, lcProjExtent)

                  } else {
                    throw new Exception("Land cover layer not supplied");
                  }

                  // Load the DEM raster with first returns
                  lcName =  "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/dem_50cm_a1_ft_story_tile1.tif"

                  if (lcName.length > 0) {
                    bytesToUse = Files.readAllBytes(Paths.get(lcName.toString))
                  } else {
                    throw new Exception("Land cover layer not supplied");
                  }

                  // Construct an object with instructions to fetch the raster
                  // Returns SingleBandGeoTiff
                  val firstDemSBGT = SinglebandGeoTiff(bytesToUse)
                  // Project First Return DEM Raster to WebMercator
                  val firstProjectedRaster = firstDemSBGT.projectedRaster
                  val ProjectedRaster( Raster(firstTile, firstExtent), firstCRS) = firstProjectedRaster.reproject(WebMercator)
                  // Create Geotiff with new projection
                  val firstReprojGeoTiff = SinglebandGeoTiff( firstTile, firstExtent, firstCRS, firstDemSBGT.tags, firstDemSBGT.options )
                  val firstDemRS = RasterSource(firstReprojGeoTiff, firstExtent)

                  // Need to cut landcover down to size of DEM
                  val nwPt = firstReprojGeoTiff.raster.rasterExtent.extent.northWest
                  val sePt = firstReprojGeoTiff.raster.rasterExtent.extent.southEast

                  // Now Crop LandCover to DEM Tile
                  var lcTile = lcReprojGeoTiff.tile
                  val (minX, minY) = lcReprojGeoTiff.raster.rasterExtent.mapToGrid(nwPt.x, nwPt.y)
                  val (maxX, maxY) = lcReprojGeoTiff.raster.rasterExtent.mapToGrid(sePt.x, sePt.y)
                  var lcCroppedTile = lcTile.crop(minX, minY, maxX, maxY)

                  val newCols: Int = firstReprojGeoTiff.tile.cols
                  val newRows: Int = firstReprojGeoTiff.tile.rows
                  val resampledTile = lcCroppedTile.resample(lcReprojGeoTiff.extent, newCols, newRows, NearestNeighbor)
                  // Finally reclassify Water based land areas
                  var tileModified: Tile = resampledTile.map({ (col: Int, row: Int, z: Int) =>

                    var zz = 1
                    // 9   Wetland, Permanent Herbaceous
                    // 10  Wetland, Mangrove
                    // 11  Water
                    // 14  Wetland, Swamp
                    if (z == 9 || z == 11 || z == 10 || z == 14) {
                      // Set the cell to visible
                      zz = 0
                    }

                    zz
                  })


                  // Write out new land cover
                  val waterCoverGT = GeoTiff( Raster(tileModified, firstExtent), firstCRS )

                  // Make Water Raster
                  val noWaterRS = RasterSource (waterCoverGT, firstExtent )

                  val cellsize = 1.0
                  val slope = 15.0

                  val meanRS = FocalRasterSourceMethodExtensions( firstDemRS ).focalMean(Square(cellsize.toInt))

                  // http://www.greenbeltconsulting.com/articles/relationships.html
                  // Calculated as percent of slope
                  val slopeRS = ElevationRasterSourceMethodExtensions( meanRS ).slope(1.0)

                  // We have slope an land cover mask out data that is water
                  val obsOutRS = LocalRasterSourceMethodExtensions(slopeRS).localMultiply( noWaterRS )

                  // Determine cells with less than slope supplied
                  val hlzRS = obsOutRS.localMapDouble { x => if ( x > 0 && x < slope.toDouble) 1.0 else 0.0 }

                  println("Verifying")
                  // Apply gainst original RasterSource
                  val result = hlzRS

                  // Write out slope product
                  val lclTile = result.get


                  val lclExtent = result.rasterExtent.get.extent
                  val gtOut = GeoTiff(lclTile, firstExtent, firstCRS )

                  val intBasedClassifierMap =
                    ColorMap(
                      Map(
                        0.0 -> RGBA(0x00000000).int,
                        1.0 -> RGBA(0x00FF00FF).int
                      )
                    )

                  var png = lclTile.renderPng( intBasedClassifierMap )
                  png.write( "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/hlzout.png")

                  // Project Bare DEM Raster to LatLng
                  val gtOutPr = gtOut.projectedRaster
                  val ProjectedRaster( Raster(gtProjTile, gtProjExtent), gtProjCRS) = gtOutPr.reproject(LatLng)
                  // Create Geotiff with new projection
                  lcReprojGeoTiff = SinglebandGeoTiff( gtProjTile, gtProjExtent, gtProjCRS, gtOut.tags, gtOut.options )
                  lcReprojGeoTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/" + "hlz.tif")

                  var fm = result.focalSum(Circle(30.0))

                  var fmTile = fm.get
                  val gtFmGT = GeoTiff(fmTile, firstExtent, firstCRS )

                  val cellSize=  gtFmGT.raster.rasterExtent.cellSize
                  println( "height" + cellSize.height + " width " + cellSize.width + " res " + cellSize.resolution)

                  val gtFmPr = gtFmGT.projectedRaster
                  val ProjectedRaster( Raster(gtFmPrTile, gtFmPrjExtent), gtFmPrjCRS) = gtOutPr.reproject(LatLng)

                  // Create Geotiff with new projection
                  lcReprojGeoTiff = SinglebandGeoTiff( gtFmPrTile, gtFmPrjExtent, gtFmPrjCRS, gtOut.tags, gtOut.options )

                  lcReprojGeoTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/" + "fm.tif")

                  "Done"
                }
              }
            }
          } ~ path("hlzlcl2") {
            parameters('src) {
              (src) => {

                complete {

                  var slopeToUse: SinglebandGeoTiff = null
                  var bytesToUse: Array[Byte] = null
                  var lcSbGt: SinglebandGeoTiff = null
                  var lcProjRS: RasterSource = null
                  var lcProjTile = null
                  var lcReprojGeoTiff: SinglebandGeoTiff = null

                  // Load the DEM raster with first returns
                  var lcName = "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/obstructGone.tif"
                  if (lcName.length > 0) {
                    bytesToUse = Files.readAllBytes(Paths.get(lcName.toString))
                  } else {
                    throw new Exception("Land cover layer not supplied");
                  }

                  println("Read in: " + lcName)
                  // Process Obstruction Removed DEM
                  val obsRemovedSBGT = SinglebandGeoTiff(bytesToUse)
                  // Project Obstruction DEM Raster to WebMercator
                  val obsProjectedRaster = obsRemovedSBGT.projectedRaster
                  val ProjectedRaster( Raster(obsTile, obsExtent), obsCRS) = obsProjectedRaster.reproject(WebMercator)
                  // Create Geotiff with new projection
                  val obsReprojGeoTiff = SinglebandGeoTiff(obsTile, obsExtent, obsCRS, obsRemovedSBGT.tags,
                    obsRemovedSBGT.options)
                  val obsDemRS = RasterSource(obsProjectedRaster, obsExtent)


                  val cellsize = 1.0
                  val slope = 15.0

                  val meanRS = FocalRasterSourceMethodExtensions(obsDemRS).focalMean(Square(cellsize.toInt))

                  // http://www.greenbeltconsulting.com/articles/relationships.html
                  // Calculated as percent of slope
                  val slopeRS = ElevationRasterSourceMethodExtensions(meanRS).slope(1.0)

                  // Determine cells with less than slope supplied, map as 1 or 0
                  val hlzRS = slopeRS.localMapDouble { x => if (x > 0 && x < slope.toDouble) 1.0 else 0.0 }

                  // Write out slope product
                  val lclTile = hlzRS.get
                  val lclExtent = hlzRS.rasterExtent.get.extent
                  val gtOut = GeoTiff(lclTile, obsExtent, obsCRS)

                  val cellSize = gtOut.raster.rasterExtent.cellSize
                  println("height" + cellSize.height + " width " + cellSize.width + " res " + cellSize.resolution)

                  // https://github.com/geotrellis/geotrellis/blob/master/docs/raster/rendering.md#color-classification
                  val intBasedClassifierMap =
                    ColorMap(
                      Map(
                        0 -> RGBA(255, 0, 0,255).int,
                        1 -> RGBA(0x00FF00FF).int
                      )
                    )

                  var png = gtOut.tile.renderPng(intBasedClassifierMap)
                  png.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/hlzout.png")

                  // Project HLZ DEM Raster to WebMercator
                  val gtOutPr = gtOut.projectedRaster
                  val ProjectedRaster( Raster(gtProjTile, gtProjExtent), gtProjCRS) = gtOutPr.reproject(LatLng)
                  // Create Geotiff with new projection
                  lcReprojGeoTiff = SinglebandGeoTiff(gtProjTile, gtProjExtent, gtProjCRS, gtOut.tags, gtOut.options)
                  lcReprojGeoTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/" + "hlz.tif")

                  // Now use focalSum to get cell values with radius that can support landing
                  var radius = 50.0 / 2
                  radius = radius / cellSize.resolution
                  var neighborSize = 1.0 / cellSize.resolution

                  var fm = hlzRS.focalSum(Circle(radius))
                  var fmTile = fm.get
                  val gtFmGT = GeoTiff(fmTile, obsExtent, obsCRS)

                  val re = gtFmGT.raster.rasterExtent

                  // Create Rastersource to get histogram
                  val fsRS = RasterSource(gtFmGT, obsExtent)

                  val histme = fsRS.histogram().get
                  println("Got History")
                  val breaks = histme.quantileBreaks(10)
                  val topBreak = breaks(9)
                  val outerBreak = breaks(8)
                  val extreameOuter = breaks(7)
                  val maxValue = histme.maxValue()

                 // val fsRSNormalized = fsRS.localMapDouble { x => if (x >= maxValue ) 1.0 else 0.0 }

                  val fsRSNormalized = fsRS.localMapDouble { x => if (
                    x >= topBreak ) {
                      3.0
                    } else if ( x >= outerBreak ) {
                      2.0
                    } else if ( x >= extreameOuter ) {
                      1.0
                    } else {
                      0.0
                    }
                  }


                  //Write out normalized
                  var theTile = fsRSNormalized.get
                  val fsNormalizedOut = GeoTiff( Raster(theTile, obsExtent), obsCRS)
                  val fsNormalizedOutPR = fsNormalizedOut.projectedRaster
                  val ProjectedRaster( Raster(fsProjTile, fsProjExtent), fsProjCRS) = fsNormalizedOutPR.reproject(LatLng)
                  // Create Geotiff with new projection
                  val fsNormalizedOutLatLng = SinglebandGeoTiff(fsProjTile, fsProjExtent, fsProjCRS, gtOut.tags, gtOut.options)

                  fsNormalizedOutLatLng.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/compare.tiff")

                  val intBasedClassifierMap2 = ColorMap(
                      Map(
                        0 -> RGBA(255, 255, 255,0).int,
                        1 -> RGBA(0xFFFF667F).int,
                        2 -> RGBA(0xFFCC667F).int,
                        3 -> RGBA(0x00FF007F).int
                      )
                    )

                  png = fsNormalizedOut.tile.renderPng(intBasedClassifierMap2 )
                  png.write( "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/compare.png")


                  //================== Regions =================
/*
                  val RegionGroupResult(regions,regionMap) = theTile.regionGroup

                  val regionCounts = mutable.Map[Int,mutable.Set[Int]]()
                  for (col <- 0 until theTile.cols) {
                    for (row <- 0 until theTile.rows) {
                      val v = theTile.get(col,row)
                      val region = regions.get(col,row)

                      if(isNoData(v)) {  }
                      else {
                        if(!regionCounts.contains(v)) { regionCounts(v) = mutable.Set[Int]() }
                                 regionCounts(v) += region
                      }
                    }
                  }

                  printf ( "Length:" + regionCounts.size )

          //        regionCounts.foreach { case ( myId:Int, mySet:Set[Int]) =>

            //          println( "Region: " + myId + " has "  + mySet.size  + " groupings.")

              //    }


                  regionCounts.foreach[Unit]( (t2) =>

                    println( "Region: " + t2._1 + " has "  + t2._2.size + " groupings.")

                  )
*/
                  var geoModified3: Tile = theTile.mapDouble { (col: Int, row: Int, z: Double) =>

                    var zz = z
                    if (z != 3) {
                      zz = NODATA
                    }
                    zz
                  }

                  val simpleOut = GeoTiff( Raster(geoModified3, obsExtent), obsCRS)

                  simpleOut.write( "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/simple.tiff")

                  val simpleColorsMap =
                    ColorMap(
                      Map(
                        NODATA -> RGBA( 255, 255, 255,0 ).int,
                        3 -> RGBA( 0xFFFF667F ).int
                      )
                    )

                  png = simpleOut.tile.renderPng( simpleColorsMap)
                  png.write( "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/simple.png")


                  val test = geoModified3.toVector( simpleOut.rasterExtent.extent)




                //  val rgrs:SeqDataSource[RegionGroupResult] = theTile.map(RegionGroup(_))
/*
                  // ===================== Polygon ====================

                  val p = Polygon(
                    Line(re.gridToMap(1000, 1000), re.gridToMap(3000, 1000), re.gridToMap(3000, 3000), re.gridToMap(1000, 3000), re.gridToMap(1000, 1000))
                  )

                  val p1 = Polygon(
                    Line( re.gridToMap(0, 0), re.gridToMap(400, 0),re.gridToMap (400, 400),re.gridToMap (0, 400), re.gridToMap(0, 0))
                  )

                  //val square  = Polygon( Line( re.gridToMap(5,9), re.gridToMap(40, 9), re.gridToMap(40,50),
                   // re.gridToMap(30,50),  re.gridToMap(5,9)) )
                  val r1:Tile = VectorToRaster.rasterize( p, re, 0x55)
                  val r2:Tile = VectorToRaster.rasterize( p1, re, 0x55)

                  val points = Seq(
                    Point(re.gridToMap(100,100)).buffer(30),
                    Point(re.gridToMap(200,200)).buffer(30),
                    Point(re.gridToMap(300,300)).buffer(30),
                    Point(re.gridToMap(400,400)).buffer(30),
                    Point(re.gridToMap(500,500)).buffer(30)
                  )
*/

                  // Collect Points
                  val points = mutable.ListBuffer[Point]()
                  theTile.foreach { (col: Int, row: Int, z: Int) =>

                    if (z == 3.0 ) {
                      val (x, y) = re.gridToMap(col, row)
                      points += Point(x, y)
                    }

                  }

                  println( "Point length: " + points.length)
                  val outerTile = SupportUtils.generateSymbolTile( points, re, 100, 50, obsCRS )
                  val middleTile = SupportUtils.generateSymbolTile( points, re, 110, 40, obsCRS )
                  val innerTile = SupportUtils.generateSymbolTile( points, re, 120, 30, obsCRS )

                  val fsNormalizedOutRS = RasterSource( fsNormalizedOut, obsExtent)
                  var combinedRS:RasterSource = fsNormalizedOutRS.localIf(outerTile, (a:Double, b:Double) => b == 100, 100)
                  combinedRS= combinedRS.localIf(middleTile, (a:Double, b:Double) => b == 110, 110)
                  combinedRS= combinedRS.localIf(innerTile, (a:Double, b:Double) => b == 120, 120)

                  //val combinedRS = fsRSNormalized
                  val combinedTile:Tile = combinedRS.get

                  val combinedTileOut = GeoTiff( combinedTile, obsExtent, obsCRS )

                  /*

                  // Collect Points
                  val points = mutable.ListBuffer[PointFeature[Int]]()
                  var count = 0
                  theTile.foreach { (col: Int, row: Int, z: Int) =>

                    if (z == 1.0 && count < 10) {
                      val (x, y) = re.gridToMap(col, row)
                      points += PointFeature(Point(x, y), z)
                      count += 1
                    }

                  }

                  println("The number of points found:" + points.length )




                  val p = Polygon(
                    Line((0, 0), (400, 0), (400, 400), (0, 400), (0, 0)),
                    Line((100, 100), (300, 100), (300, 300), (100, 300), (100, 100))
                  )


                //  val r1:Tile = VectorToRaster.rasterize( points(0).geom, re, 0x55 )

                  var geoModified2: Tile = r1.mapDouble { (col: Int, row: Int, z: Double) =>

                    var zz = z
                    if (z != 0x55) {
                      zz = 0
                    } else {
                      println("match")
                    }
                    zz
                  }

                  val r1GT = GeoTiff( r1, obsExtent, obsCRS )

                  val fsNormalizedOutRS = RasterSource( fsNormalizedOut, obsExtent)
                  val r1RS = RasterSource ( r1GT, obsExtent )

                  val combinedRS:RasterSource = fsNormalizedOutRS.localOr( r1RS )
                  //val combinedRS = fsRSNormalized
                  val combinedTile:Tile = combinedRS.get

                  val combinedTileOut = GeoTiff( combinedTile, obsExtent, obsCRS )


                  val createdTile:Tile = DoubleArrayTile(Array.ofDim[Double](theTile.rows*theTile.cols).fill(0),  theTile.cols, theTile.rows)

                  println( "The points:" + points.length)
                  val bufferAmount = 100.0 // map units
                  points.map(_.buffer(bufferAmount))
                  println("Bogus4")

                  points.foreach( f=> {
                    //var (re.mapToGrid( f.data.x)
                    var something:Tile = Rasterizer.rasterizeWithValue( f.geom, re, 10 )
                    createdTile.combineDouble(something) { (z1, z2) => z1 + z2 }

                  })

                  createdTile.foreachDouble( f=> {
                      if ( f == 10 )
                        println( "Have Value ")
                  })


                  val fsNormalizedOut = GeoTiff( r1, re.extent, obsCRS )
                  val ProjectedRaster( fsPrjTile, fsPrjExtent, fsPrjCRS) = fsNormalizedOut.reproject(LatLng)
                  val fsReprojGeoTiff = SingleBandGeoTiff( fsPrjTile, fsPrjExtent, fsPrjCRS, gtOut.tags, gtOut.options )
                  */
                  val hlzColorClassifierrMap =
                    ColorMap(
                      Map(
                        0 -> RGBA(255, 255, 255,0).int,
                        1 -> RGBA(0xFFFF6600).int,
                        2 -> RGBA(0xFFCC6600).int,
                        3 -> RGBA(0x00FF0000).int,
                        100 -> RGBA(0xFFFF667F).int,
                        110 -> RGBA(0xFFCC667F).int,
                        120 -> RGBA(0x00FF007F).int
                      )
                    )

                  val combinedTileOutRP = combinedTileOut.projectedRaster
                  val ProjectedRaster( Raster(fsPrjTile, fsPrjExtent), fsPrjCRS) = combinedTileOutRP.reproject(LatLng)
                  val combinedTileOutTiff = SinglebandGeoTiff( fsPrjTile, fsPrjExtent, fsPrjCRS, gtOut.tags, gtOut.options )

                  png = combinedTileOut.tile.renderPng( hlzColorClassifierrMap )

                  png.write( "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/focalMaxout2.png")

                  combinedTileOutTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/" + "fm.tif")

                  "Done"
                }
              }
            }
          } ~ path("hlzlcl3") {
            parameters('src) {
              (src) => {

                complete {

                  var errcode:Int = 0 ;

                  var bytesToUse: Array[Byte] = null
                  var demReprojGeoTiff: SinglebandGeoTiff = null
                  var demCRS: CRS = null
                  var demreturnCreated = false
                  var demRS: RasterSource = null;
                //  var lcReprojGeoTiff : SingleBandGeoTiff = null
                  var lcSbGt : SinglebandGeoTiff = null
                  var lcProjRS : RasterSource = null;
                  var demExtent : Extent = null
                  var landCoverObstructionCreated = false
                  var obstructGoneTiff: SinglebandGeoTiff = null
                  var obsOutRS: RasterSource = null
                  try {

                    try {


                      // Load the DEM raster with first returns
                      var lcName = "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/dem_50cm_a1_ft_story_tile1.tif"
                      if (lcName.length > 0) {
                        bytesToUse = Files.readAllBytes(Paths.get(lcName.toString))
                      } else {
                        throw new Exception("Land cover layer not supplied");
                      }

                      val firstDemSBGT = SinglebandGeoTiff(bytesToUse)

                      // Project firstrerturn DEM Raster to LatLng
                      val firstProjectedRaster = firstDemSBGT.projectedRaster
                      val ProjectedRaster(Raster(firstTile, firstExtent), firstCRS) = firstProjectedRaster.reproject(LatLng)

                      // Create Geotiff with new projection
                      demReprojGeoTiff = SinglebandGeoTiff(firstTile, firstExtent, firstCRS, firstDemSBGT.tags, firstDemSBGT.options)

                      demExtent = firstExtent
                      demCRS = firstCRS
                      demRS = RasterSource(demReprojGeoTiff, firstExtent)
                      //firstExtentToUse = firstExtent
                      demreturnCreated = true

                    } catch {
                      case e: Exception =>
                        demreturnCreated = false
                        throw new IllegalArgumentException("First Return IO exception will not be included in return")
                    }

                    try {

                      // Load the DEM raster with first returns
                      var lcName = "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/mergedlandcover2.tif"
                      if (lcName.length > 0) {
                        bytesToUse = Files.readAllBytes(Paths.get(lcName.toString))
                      } else {
                        throw new Exception("Land cover layer not supplied");
                      }

                      lcSbGt = SinglebandGeoTiff(bytesToUse)


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
                      var lcTile = lcSbGt.tile
                      val (minX, minY) = lcSbGt.raster.rasterExtent.mapToGrid(nwPt.x, nwPt.y)
                      val (maxX, maxY) = lcSbGt.raster.rasterExtent.mapToGrid(sePt.x, sePt.y)
                      var lcCroppedTile = lcTile.crop(minX, minY, maxX, maxY)

                      val newCols: Int = demReprojGeoTiff.tile.cols
                      val newRows: Int = demReprojGeoTiff.tile.rows
                      val resampledTile = lcCroppedTile.resample(lcSbGt.extent, newCols, newRows, NearestNeighbor)

                      resampledTile.renderPng(ColorRamps.GreenToRedOrange).write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/croppedlc.png")

                      // Finally reclassify Landcover based land areas
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

                      // Landcover RasterSource
                      val lcCoverObstructionGT = GeoTiff( tileModified, demExtent, demCRS )
                      val lcRS = RasterSource (lcCoverObstructionGT, demExtent )

                      // We have slope an land cover mask out data that is not sutitable
                      obsOutRS = LocalRasterSourceMethodExtensions(demRS).localMultiply( lcRS )
                      obsOutRS = LocalRasterSourceMethodExtensions(obsOutRS).localAbs()

                      obsOutRS = LocalRasterSourceMethodExtensions(obsOutRS).localMapDouble {
                        x => if ( x > 0 ) x else NODATA }

                      obstructGoneTiff = GeoTiff(obsOutRS.get, obsOutRS.rasterExtent.get.extent, demCRS)


                      landCoverObstructionCreated = true
                    } catch {
                      case e: Exception =>
                        throw new IllegalArgumentException("Landcover Obstruction Layer IO exception. Will not be included in return")
                    }

                    if (landCoverObstructionCreated ) {
                      obstructGoneTiff.tile.renderPng(ColorRamps.GreenToRedOrange).write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/lcobs.png")
                      obstructGoneTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/lcobs.tiff")
                    }


                    val radius = 50 / 2
                    val cellsize =  1.0
                    val slope =  15.0

                    val meanRS= FocalRasterSourceMethodExtensions(obsOutRS).focalMean(Square(cellsize.toInt))

                    // http://www.greenbeltconsulting.com/articles/relationships.html
                    // Calculated as percent of slope
                    val slopeRS = ElevationRasterSourceMethodExtensions(obsOutRS).slope(0.00000898)
                    val slopeClassificationRS = LocalRasterSourceMethodExtensions(slopeRS).localMapDouble {
                      x => if ( x > 0 && x < slope.toDouble) 1.0 else 0.0 }
                    val rasterCellResolution =  obstructGoneTiff.raster.rasterExtent.cellSize.resolution

                    // Now use focalSum to get cell values with radius that can support landing
                    var neighborSize = radius // rasterCellResolution
                    var focalsumRS = slopeClassificationRS.focalSum(Circle(neighborSize))
                    var fmTile = focalsumRS.get
                    val gtFmGT = GeoTiff(fmTile, obstructGoneTiff.extent, demCRS)

                    val re = gtFmGT.raster.rasterExtent
                    val fsRS = RasterSource(gtFmGT, obstructGoneTiff.extent)

                    //var focalsumRS = FocalRasterSourceMethodExtensions(slopeClassificationRS).focalSum( Circle(neighborSize) )

                    val hist = fsRS.histogram().get
                    val breaks = hist.quantileBreaks(10)
                    val topBreak = breaks(9)
                    val outerBreak = breaks(8)
                    val extreameOuter = breaks(7)
                    val maxValue = hist.maxValue()


                    val fsRSNormalized = focalsumRS.localMapDouble { x => if (
                        x >= topBreak ) {
                        3.0
                      } else if ( x >= outerBreak ) {
                        2.0
                      } else if ( x >= extreameOuter ) {
                        1.0
                      } else {
                        0.0
                      }
                    }


                    //Write out normalized
                    var theTile = fsRSNormalized.get
                    val fsNormalizedOut = GeoTiff(theTile, focalsumRS.rasterExtent.get.extent, demCRS)
                    val fsNormalizedOutPR = fsNormalizedOut.projectedRaster
                    val ProjectedRaster( Raster(fsProjTile, fsProjExtent), fsProjCRS) = fsNormalizedOutPR.reproject(LatLng)
                    // Create Geotiff with new projection
                    val fsNormalizedOutLatLng = SinglebandGeoTiff(fsProjTile, fsProjExtent, fsProjCRS)

                    fsNormalizedOutLatLng.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/compare3.tiff")

                    val intBasedClassifierMap =
                      ColorMap(
                        Map(
                          0 -> RGBA(255, 255, 255,0).int,
                          1 ->  RGBA(0xFFFF667F).int,
                          2 -> RGBA(0xFFCC667F).int,
                          3 -> RGBA(0x00FF007F).int
                        )
                      )

                    var png = fsNormalizedOut.tile.renderPng( intBasedClassifierMap )
                    png.write( "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/compare3.png")


                    var geoModified3: Tile = theTile.mapDouble { (col: Int, row: Int, z: Double) =>

                      var zz = z
                      if (z != 3) {
                        zz = NODATA
                      }
                      zz
                    }

                    val simpleOut = GeoTiff(geoModified3, focalsumRS.rasterExtent.get.extent, demCRS)

                    simpleOut.write( "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/simple3.tiff")

                    val simpleColorsMap =
                      ColorMap(
                        Map(
                          NODATA -> RGBA( 255, 255, 255,0 ).int,
                          3 -> RGBA( 0xFFFF667F ).int
                        )
                      )

                    png = simpleOut.tile.renderPng( simpleColorsMap )
                    png.write( "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/simple3.png")

                    // Collect Points
                    val points = mutable.ListBuffer[Point]()
                    theTile.foreach { (col: Int, row: Int, z: Int) =>

                      if (z == 3.0 ) {
                        val (x, y) = re.gridToMap(col, row)
                        points += Point(x, y)
                      }

                    }

                    println( "Point length: " + points.length)
                    val outerTile = SupportUtils.generateSymbolTile( points, re, 100, 50, demCRS )
                    println( "Outer")
                    val middleTile = SupportUtils.generateSymbolTile( points, re, 110, 40, demCRS )
                    println( "Middle")
                    val innerTile = SupportUtils.generateSymbolTile( points, re, 120, 30, demCRS )
                    println( "Inner")

                    val fsNormalizedOutRS = RasterSource( fsNormalizedOut, focalsumRS.rasterExtent.get.extent)
                    var combinedRS:RasterSource = fsNormalizedOutRS.localIf(outerTile, (a:Double, b:Double) => b == 100, 100)
                    combinedRS= combinedRS.localIf(middleTile, (a:Double, b:Double) => b == 110, 110)
                    combinedRS= combinedRS.localIf(innerTile, (a:Double, b:Double) => b == 120, 120)
                    println( "Combine")

                    //val combinedRS = fsRSNormalized
                    val combinedTile:Tile = combinedRS.get

                    println( "Combine Out")
                    val combinedTileOut = GeoTiff( Raster(combinedTile, focalsumRS.rasterExtent.get.extent), demCRS )

                    val hlzColorClassifierrMap =
                      ColorMap(
                        Map(
                          0 -> RGBA(255, 255, 255,0).int,
                          1 -> RGBA(0xFFFF6600).int,
                          2 -> RGBA(0xFFCC6600).int,
                          3 -> RGBA(0x00FF0000).int,
                          100 -> RGBA(0xFFFF667F).int,
                          110 -> RGBA(0xFFCC667F).int,
                          120 -> RGBA(0x00FF007F).int
                        )
                      )

                    val combinedTileRP = combinedTileOut.projectedRaster
                    val ProjectedRaster( Raster(fsPrjTile, fsPrjExtent), fsPrjCRS) = combinedTileRP.reproject(LatLng)
                    val combinedTileOutTiff = SinglebandGeoTiff( fsPrjTile, fsPrjExtent, fsPrjCRS)

                    println( "Write out files")
                    png = combinedTileOut.tile.renderPng( hlzColorClassifierrMap)

                    png.write( "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/focalMaxout3.png")

                    combinedTileOutTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/" + "fm3.tif")

                  } catch {

                    case e: Exception =>
                      errcode = 1
                      println( "Exception in build obstruction layer, no layers stored" + e.getMessage)
                    case e: IllegalArgumentException =>
                      errcode = 1
                      println( "Exception in build obstruction layer, no layers stored" + e.getMessage)
                  }

                  errcode

                  "Done"
                }
              }
            }
          }~path("hlzlcl4") {
          parameters('src) {
            (src) => {

              complete {

                var slopeToUse:SinglebandGeoTiff = null
                var bytesToUse:Array[Byte] = null
                var lcSbGt:SinglebandGeoTiff = null
                var lcProjRS:RasterSource = null
                var lcProjTile = null
                var lcReprojGeoTiff:SinglebandGeoTiff = null

                // Load The LandCover Layer
                var lcName = "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/n36w077_fme_m.tif"

                if (lcName.length > 0) {
                  bytesToUse = Files.readAllBytes(Paths.get(lcName.toString))
                  lcSbGt = SinglebandGeoTiff(bytesToUse)

                  // Project Land Coverto WebMercator
                  val lcProjectedRaster = lcSbGt.projectedRaster
                  val ProjectedRaster( Raster(lcProjTile, lcProjExtent), lcProjCRS) = lcProjectedRaster.reproject(WebMercator)
                  // Create Geotiff with new projection
                  lcReprojGeoTiff = SinglebandGeoTiff( lcProjTile, lcProjExtent, lcProjCRS, lcSbGt.tags, lcSbGt.options )
                  lcProjRS = RasterSource( lcReprojGeoTiff, lcProjExtent)
                  lcReprojGeoTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/lcreprojected.tiff")


                } else {
                  throw new Exception("Land cover layer not supplied");
                }

                // Load the DEM raster with first returns
                lcName =  "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/dem_50cm_a1_ft_story_tile1.tif"

                if (lcName.length > 0) {
                  bytesToUse = Files.readAllBytes(Paths.get(lcName.toString))
                } else {
                  throw new Exception("Land cover layer not supplied");
                }

                // Construct an object with instructions to fetch the raster
                // Returns SingleBandGeoTiff
                val firstDemSBGT = SinglebandGeoTiff(bytesToUse)
                // Project First Return DEM Raster to LATLNG
                val firstProjectedRaster = firstDemSBGT.projectedRaster
                val ProjectedRaster(Raster(firstTile, firstExtent), firstCRS) = firstProjectedRaster.reproject(WebMercator)
                // Create Geotiff with new projection
                val firstReprojGeoTiff = SinglebandGeoTiff( firstTile, firstExtent, firstCRS, firstDemSBGT.tags, firstDemSBGT.options )
                firstReprojGeoTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/firstdemreporjected.tiff")
                val firstDemRS = RasterSource(firstReprojGeoTiff, firstExtent)

                // Need to cut landcover down to size of DEM
                val nwPt = firstReprojGeoTiff.raster.rasterExtent.extent.northWest
                val sePt = firstReprojGeoTiff.raster.rasterExtent.extent.southEast

                // Now Crop LandCover to DEM Tile
                var lcTile = lcReprojGeoTiff.tile
                val (minX, minY) = lcReprojGeoTiff.raster.rasterExtent.mapToGrid(nwPt.x, nwPt.y)
                val (maxX, maxY) = lcReprojGeoTiff.raster.rasterExtent.mapToGrid(sePt.x, sePt.y)
                var lcCroppedTile = lcTile.crop(minX, minY, maxX, maxY)

                val newCols: Int = firstReprojGeoTiff.tile.cols
                val newRows: Int = firstReprojGeoTiff.tile.rows
                val resampledTile = lcCroppedTile.resample(lcReprojGeoTiff.extent, newCols, newRows, NearestNeighbor)
                // Finally reclassify Water based land areas
                // Finally reclassify Landcover based land areas
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
                val waterCoverGT = GeoTiff( tileModified, firstExtent, firstCRS )

                waterCoverGT.tile.renderPng( ColorRamps.LightToDarkGreen).write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/lcobs.png")

                // Make Water Raster
                val noWaterRS = RasterSource (waterCoverGT, firstExtent )

                // We have slope an land cover mask out data that is water
                val obsOutRS = LocalRasterSourceMethodExtensions(firstDemRS).localMultiply( noWaterRS )

                val obstructGoneTiff = GeoTiff(obsOutRS.get, firstExtent, firstCRS)

                val obsProjectRaster = obstructGoneTiff.projectedRaster
                val ProjectedRaster( Raster(obsgTile, obsgExtent), obsgCRS) = obsProjectRaster.reproject(LatLng)
                // Create Geotiff with new projection
                val obsgReprojGeoTiff = SinglebandGeoTiff( obsgTile, obsgExtent, obsgCRS, obstructGoneTiff.tags, obstructGoneTiff.options )
                val obsgProjRS = RasterSource( obsgReprojGeoTiff, obsgExtent)


                obsgReprojGeoTiff.tile.renderPng( ColorRamps.LightToDarkGreen).write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/lcobs2.png")
                obsgReprojGeoTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/lcobs.tiff")

                val radius = 25/ 2

                println( "HLZ 1")
                val cellsize =  1.0
                val slope =  15.0

                val meanRS = FocalRasterSourceMethodExtensions(obsOutRS).focalMean(Square(cellsize.toInt))
                println( "HLZ 2")
                // http://www.greenbeltconsulting.com/articles/relationships.html
                // Calculated as percent of slope
                val slopeRS = ElevationRasterSourceMethodExtensions(meanRS).slope(1)
                println( "HLZ 3")
                val slopeClassificationRS = LocalRasterSourceMethodExtensions(slopeRS).localMapDouble {
                  x => if ( x > 0 && x < slope.toDouble) 1.0 else 0.0 }

                val simpleColorsMap =
                  ColorMap(
                    Map(
                      NODATA -> RGBA( 255, 255, 255,0 ).int,
                      3 -> RGBA( 0xFFFF667F ).int
                    )
                  )

                slopeClassificationRS.get.renderPng(simpleColorsMap).write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/slopeclass.png")

                // Determine cells with less than slope supplied, map as 1 or 0
                //val slopeClassificationRS = slopeRS.localMapDouble { x => if ( x > 0 && x < slope.toDouble) 1.0 else 0.0 }
                println( "HLZ 4")
                //val rasterCellResolution =  slopeClassificationRSS.metaData.rasterExtent.cellSize.resolution

                // Now use focalSum to get cell values with radius that can support landing
                var neighborSize = radius // rasterCellResolution
                var focalsumRS = FocalRasterSourceMethodExtensions(slopeClassificationRS).focalSum( Circle(neighborSize) )

                var fmTile = focalsumRS.get
                val gtFmGT = GeoTiff(fmTile, obstructGoneTiff.extent, firstCRS)

                val re = gtFmGT.raster.rasterExtent
                val fsRS = RasterSource(gtFmGT, obstructGoneTiff.extent)
                //focalsumRS.get.renderPng(ColorRamps.BlueToRed).write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/focalsumclass.png")

                println( "HLZ 5")
                var hist = focalsumRS.histogram().get
                println("Got Hist")
                val breaks = hist.quantileBreaks(10)
                println("Got Breaks: " + breaks.length)
                val topBreak = breaks( breaks.length - 1 )
                println("Got Top Break : " + topBreak)
                val outerBreak = breaks(breaks.length - 2)
                println("Got Outer Break : " + outerBreak)
                val extreameOuter = breaks(breaks.length - 3)
                println("Got Extreme OuterBreak : " + extreameOuter )
                val maxValue = hist.mean()
                println("MaxValue: " + maxValue)



                "Done"
              }
            }
          }
        }~
        path("aspectlcl") {
            parameters('src) {
              (src) => {

                complete {


                  val fileList = LocalFileAccess.getListOfFiles( "/Users/chrismangold/develop/preparedata/loudon/wgs84tifs")

                  for (name <- fileList) {

                    println(name)
                    if ( name.toString.contains( ".tif") ) {

                      val bytesToUse = Files.readAllBytes(Paths.get(name.toString))

                      // Construct an object with instructions to fetch the raster
                      // Returns SingleBandGeoTiff
                      val gtIn = SinglebandGeoTiff(bytesToUse)

                      // Move from tile to rastersource
                      // Maintain Projections
                      val theRS = RasterSource(gtIn, gtIn.extent)

                      // Generate Aspect
                      val aspectRS = ElevationRasterSourceMethodExtensions( theRS ).aspect

                      // Write out slope product
                      val lclTile = aspectRS.get
                      val lclExtent = aspectRS.rasterExtent.get.extent
                      val gtOut = GeoTiff(lclTile, lclExtent, gtIn.crs)

                      var fileRootindex = name.toString.lastIndexOf("/");
                      var fileName = name.toString.substring(fileRootindex + 1)

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

                      val png = lclTile.renderPng(aspectClassifierMap)
                      png.write("/Users/chrismangold/develop/preparedata/loudon/aspect/" + fileName + ".png")

                      // Use when writing local
                      gtOut.write("/Users/chrismangold/develop/preparedata/loudon/aspect/" + fileName)

                    }
                  }
                  "Done"
                }
              }
            }
          } ~
          path("slopelcl") {
            parameters('src) {
              (src) => {

                complete {


                  val fileList = LocalFileAccess.getListOfFiles( "/Users/chrismangold/develop/preparedata/pipeline")


                  for (name <- fileList) {

                    println(name)
                    if ( name.toString.contains( ".tif") ) {

                      val bytesToUse = Files.readAllBytes(Paths.get(name.toString))

                      // Construct an object with instructions to fetch the raster
                      // Returns SingleBandGeoTiff
                      val gtIn = SinglebandGeoTiff(bytesToUse)

                      // Move from tile to rastersource
                      // Maintain Projections
                      val theRS = RasterSource(gtIn, gtIn.extent)

                      // Generate Slope
                      val slopeRS = ElevationRasterSourceMethodExtensions(theRS).slope(0.00000898)

                      // Write out slope product
                      val lclTile = slopeRS.get
                      val lclExtent = slopeRS.rasterExtent.get.extent
                      val gtOut = GeoTiff(lclTile, lclExtent, gtIn.crs)

                      var fileRootindex = name.toString.lastIndexOf("/");
                      var fileName = name.toString.substring(fileRootindex + 1)

                      // Use when writing local
                      gtOut.write("/Users/chrismangold/develop/preparedata/pipeline/slope/" + fileName)

                    }
                  }
                  "Done"
                }
              }
            }
          } ~
          path("artslopelcl") {
            parameters('src) {
              (src) => {

                complete {


                  val fileList = LocalFileAccess.getListOfFiles( "/Users/chrismangold/develop/preparedata/pipeline")


                  for (name <- fileList) {

                    println(name)
                    if ( name.toString.contains( ".tif") ) {

                      val bytesToUse = Files.readAllBytes(Paths.get(name.toString))

                      // Construct an object with instructions to fetch the raster
                      // Returns SingleBandGeoTiff
                      val gtIn = SinglebandGeoTiff(bytesToUse)

                      // Move from tile to rastersource
                      // Maintain Projections
                      val theRS = RasterSource(gtIn, gtIn.extent)

                      // Generate Slope
                      val slopeRS = ElevationRasterSourceMethodExtensions(theRS).slope(0.00000898)

                      // Write out slope product
                      val lclTile = slopeRS.get
                      val lclExtent = slopeRS.rasterExtent.get.extent
                      val gtOut = GeoTiff(lclTile, lclExtent, gtIn.crs)

                      var fileRootindex = name.toString.lastIndexOf("/");
                      var fileName = name.toString.substring(fileRootindex + 1)

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

                      val png = lclTile.renderPng(artClassifierMap)
                      png.write("/Users/chrismangold/develop/preparedata/pipeline/slope/" + fileName + ".png")
                      // Use when writing local
                      gtOut.write("/Users/chrismangold/develop/preparedata/pipeline/slope/" + fileName)

                    }
                  }
                  "Done"
                }
              }
            }
          } ~
         path("tpilcl") {
          parameters('src) {
            (src) => {

              complete {


                val fileList = LocalFileAccess.getListOfFiles( "/Users/chrismangold/develop/preparedata/loudon/wgs84tifs")


                for (name <- fileList) {

                  println(name)
                  if ( name.toString.contains( ".tif") ) {

                    val bytesToUse = Files.readAllBytes(Paths.get(name.toString))

                    // Construct an object with instructions to fetch the raster
                    // Returns SingleBandGeoTiff
                    val gtIn = SinglebandGeoTiff(bytesToUse)

                    // Move from tile to rastersource
                    // Maintain Projections
                    val theRS = RasterSource(gtIn, gtIn.extent)

                    // Convert from raster to tile
                    //val slopeRS = RasterSource(geoTiff.tile, geoTiff.extent )

                    val tpiMinRS = FocalRasterSourceMethodExtensions( theRS ).focalMin( Square( 1 ))

                    val tpiMaxRS = FocalRasterSourceMethodExtensions( theRS ).focalMax( Square( 1 ))

                    val tpiMeanRS = FocalRasterSourceMethodExtensions( theRS ).focalMean( Square( 1 ))

                    // Normalize smoothed raster
                    val resultNorm = tpiMeanRS -  tpiMinRS
                    val resultDivider =  tpiMaxRS / tpiMinRS

                    val tpiResult = resultNorm / resultDivider


                    // Write out slope product
                    val lclTile = tpiResult.get
                    val lclExtent = tpiResult.rasterExtent.get.extent
                    val gtOut = GeoTiff(lclTile, lclExtent, gtIn.crs)

                    var fileRootindex = name.toString.lastIndexOf("/");
                    var fileName = name.toString.substring(fileRootindex + 1)

                    // Use when writing local
                    gtOut.write("/Users/chrismangold/develop/preparedata/loudon/tpi/" + fileName)

                  }
                }
                "Done"
              }
            }
          }
        }~
          path("hillshadelcl") {
            parameters('src) {
              (src) => {

                complete {


                  val LightToDarkGrey = ColorRamp(
                    0xF4FBF6,0xF0F7F2, 0xECF3EE,0xE8EFEA,
                    0xE4EBE6, 0xE0E7E2, 0xDCE3DE, 0xD8DFDA,
                    0xD5DBD6, 0xD1D7D2, 0xCDD3C3, 0XC9CFCA,
                    0xC5CBC6, 0xC1C7C2, 0xBDC3BE, 0xBABFBA,
                    0xB6BBB6, 0xB2B7B2, 0xAEB3AE, 0xAAAFAA,
                    0xA6ABA6, 0xA2A7A2, 0x9FA39F)



                  val fileList = LocalFileAccess.getListOfFiles( "/Users/chrismangold/develop/preparedata/pipeline")

                  for (name <- fileList) {

                    println(name)
                    if ( name.toString.contains( ".tif") ) {

                      val bytesToUse = Files.readAllBytes(Paths.get(name.toString))

                      // Construct an object with instructions to fetch the raster
                      // Returns SingleBandGeoTiff
                      val gtIn = SinglebandGeoTiff(bytesToUse)

                      // Move from tile to rastersource
                      // Maintain Projections
                      val theRS = RasterSource(gtIn, gtIn.extent)

                      val hsRS = theRS.hillshade(90.0, 40.0, 1.0)
                      //val outRS = ElevationRasterSourceMethodExtensions(theRS).hillshade(90.0, 40.0, 1.0)

                      // Write out hillshade product
                      val lclTile = hsRS.get
                      val lclExtent = hsRS.rasterExtent.get.extent
                      val gtOut = GeoTiff(lclTile, lclExtent, gtIn.crs)

                     // val colorMap =
                       // ColorRamp(LightToDarkGrey).setAlphaGradient(0xff,0xff)

                      var fileRootindex = name.toString.lastIndexOf("/");
                      var fileName = name.toString.substring(fileRootindex + 1)

                      val test = ColorRamps.ClassificationBoldLandUse.toColorMap(lclTile.histogram)
                      lclTile.renderPng(test).write("/Users/chrismangold/develop/preparedata/pipeline/hillshade/" + fileName + ".png")
                      // Use when writing local
                      gtOut.write("/Users/chrismangold/develop/preparedata/pipeline/hillshade/" + fileName)

                    }
                  }
                  "Done"
                }
              }
            }
          }
      }
    }
  }

  def buildObstruction = {

    post {

      path("buildobs") {

        entity(as[DemBasePost]) { buildObjstructionrequest =>
          respondWithMediaType(MediaTypes.`application/json`) {

            complete {

              var firstDemRS : RasterSource = null ;
              var bareDemRS  : RasterSource = null ;
              var finalDemRS : RasterSource = null ;
              var finalDemExtent : Extent = null ;
              var obsOutRS : RasterSource = null ;
              var finalDemCRS : CRS = null
              var lcSbGt:SinglebandGeoTiff = null
              var bareReprojGeoTiff :SinglebandGeoTiff = null
              var processResult = false
              var bareearthStored = false
              var firstreturnStored = false
              var landcoverincluded = false
              var firstreturnobstructionsincluded = false
              var obstructionStore = false
              var processLevels = 0x0

              println("Starting....")

              try {

                val resolutionStr = buildObjstructionrequest.resolution.toString.toLowerCase
                try {

                  // Load The LandCover Layer
                  val lcName = buildObjstructionrequest.landcoverfilepath // "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/n36w077_fme_m.tif"

                  if (lcName.length > 0) {
                    var bytesToUse = Files.readAllBytes(Paths.get(lcName.toString))
                    lcSbGt = SinglebandGeoTiff(bytesToUse)

                    processLevels |= INCLUDE_LC_HANDLING
                  } else {
                    throw new Exception("Land cover layer not supplied");
                  }

                } catch {
                  case e: Exception => println("Land Cover path invalid will not be included in in output");
                }

                try {

                  // Load in the selected Bare earth DEM Tif
                  val bareDemName = buildObjstructionrequest.bareearthfilepath // "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/dem_bare_50cm_ft_story_tile1.tif"
                  var bytesToUse = Files.readAllBytes(Paths.get(bareDemName.toString))
                  val bareDemSBGT = SinglebandGeoTiff(bytesToUse)

                  // Project Bare DEM Raster to LatLng
                  val bareProjectedRaster = bareDemSBGT.projectedRaster
                  val ProjectedRaster( Raster(bareTile, bareExtent), bareCRS) = bareProjectedRaster.reproject(LatLng)
                  // Create Geotiff with new projection
                  bareReprojGeoTiff = SinglebandGeoTiff(bareTile, bareExtent, bareCRS, bareDemSBGT.tags, bareDemSBGT.options)
                  bareDemRS = RasterSource(bareReprojGeoTiff, bareExtent)
                  obsOutRS = bareDemRS

                  finalDemExtent = bareExtent
                  finalDemCRS = bareCRS

                  bareearthStored = true

                  var fileName = uuid.toString  + ".tif"
                  // val bob = AmazonConnect.BAConvert(bareReprojGeoTiff)
                  val newBob = bareReprojGeoTiff.toBytes()
                  AmazonConnect.writeS3Object( resolutionStr + "/bareearth", fileName, newBob)

                 // bareReprojGeoTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/baredem.tif")


                } catch {
                  case e: Exception => throw new IllegalArgumentException("Invalid bare dem file path");
                }


                try {

                  // Load in the selected 1st return DEM Tif
                  val firstDemName = buildObjstructionrequest.firstreturnfilepath // "/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/in/dem_50cm_a1_ft_story_tile1.tif"
                  val bytesToUse = Files.readAllBytes(Paths.get(firstDemName.toString))
                  val firstDemSBGT = SinglebandGeoTiff(bytesToUse)
                  // Project Bare DEM Raster to LatLng
                  val firstProjectedRaster = firstDemSBGT.projectedRaster
                  val ProjectedRaster( Raster(firstTile, firstExtent), firstCRS) = firstProjectedRaster.reproject(LatLng)
                  // Create Geotiff with new projection
                  val firstReprojGeoTiff = SinglebandGeoTiff(firstTile, firstExtent, firstCRS, firstDemSBGT.tags, firstDemSBGT.options)
                  firstDemRS = RasterSource(firstReprojGeoTiff, firstExtent)

                  processLevels |= INCLUDE_OBSTRUCTION_HANDLING

                  firstreturnStored = true

                  var fileName = uuid.toString  + ".tif"
                  //val bob = AmazonConnect.BAConvert(firstReprojGeoTiff)
                  val newBob = firstReprojGeoTiff.toBytes()
                  AmazonConnect.writeS3Object( resolutionStr + "/firstreturn", fileName, newBob)

                //  firstReprojGeoTiff.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/firstdem.tif")

                } catch {
                  case e: Exception => println("First Return  IO exception will not be included in return");
                }

                if ((processLevels & INCLUDE_OBSTRUCTION_HANDLING) > 0) {

                  // We have bare earth and first returb so build obstruction layer
                 // val deltaRS = LocalRasterSourceMethodExtensions(firstDemRS).localSubtract(bareDemRS)

                  obsOutRS = LocalRasterSourceMethodExtensions(bareDemRS).localIf( firstDemRS,{(z1:Double,z2:Double) =>
                    z2 > ( z1 + 1.0)}, NODATA)

                  /*
                  // Now we have a obstruction difference between 1st return and bare earth
                  val deltaTile = deltaRS.get

                  // Mask out all elevations that are greater then one and 1/2 meter
                  var tile1Meter: Tile = deltaTile.mapDouble({ (col: Int, row: Int, z: Double) =>

                    var zz = z

                    if (z >= 1.5) {
                      // Greater than 1 meter than reclassify cell as 0
                      zz = NODATA
                    }

                    zz
                  })

                  // Make a new Geotiff
                  val oneMeterSBGT = GeoTiff(tile1Meter, deltaRS.rasterExtent.get.extent, finalDemCRS)
                  val oneMeterRS = RasterSource(oneMeterSBGT, finalDemExtent)

                 // obsOutRS = LocalRasterSourceMethodExtensions(firstDemRS).localSubtract(oneMeterRS)
                  // Map Algebra take out cells with obstructions greater than 1 meter
                 // obsOutRS = LocalRasterSourceMethodExtensions(firstDemRS).localIf( deltaRS,{(z1:Double,z2:Double) =>
                   // z2 > 1.0},NODATA)
*/
                  val obstructGone = GeoTiff(obsOutRS.get, obsOutRS.rasterExtent.get.extent, finalDemCRS)
                  obstructGone.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/" + "subtractdiff.tif")


                  firstreturnobstructionsincluded = true

                }

                if ((processLevels & INCLUDE_LC_HANDLING) > 0) {

                  // Need to cut landcover down to size of DEM
                  val nwPt = bareReprojGeoTiff.raster.rasterExtent.extent.northWest
                  val sePt = bareReprojGeoTiff.raster.rasterExtent.extent.southEast


                  // Now Crop LandCover to DEM Tile
                  var lcTile = lcSbGt.tile
                  val (minX, minY) = lcSbGt.raster.rasterExtent.mapToGrid(nwPt.x, nwPt.y)
                  val (maxX, maxY) = lcSbGt.raster.rasterExtent.mapToGrid(sePt.x, sePt.y)
                  var lcCroppedTile = lcTile.crop(minX, minY, maxX, maxY)
                  val newCols: Int = bareReprojGeoTiff.tile.cols
                  val newRows: Int = bareReprojGeoTiff.tile.rows
                  val resampledTile = lcCroppedTile.resample(lcSbGt.extent, newCols, newRows, NearestNeighbor)

                  // Finally reclassify Water based land areas
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
                  val waterCoverGT = GeoTiff(tileModified, finalDemExtent, finalDemCRS)

                  // Make Water Raster
                  val noWaterRS = RasterSource(waterCoverGT, finalDemExtent)
                  landcoverincluded = true

                  // Map Algrebra take out cells with water
                  // obsOutRS = LocalRasterSourceMethodExtensions(obsOutRS).localMultiply(noWaterRS)

                  obsOutRS = LocalRasterSourceMethodExtensions(obsOutRS).localIf( noWaterRS,{(z1:Double,z2:Double) =>
                    z2 == 0 || z1 == NODATA }, NODATA)

                }

                // Set nodata for all 0 values
               // val finalRSData = obsOutRS.localMapDouble { x => if (x == 0) NODATA else x }

                // Write out scaled dem for test
                val finalDem = GeoTiff(obsOutRS.get, obsOutRS.rasterExtent.get.extent, finalDemCRS)

                var fileName = uuid.toString  + ".tif"
                // val bob = AmazonConnect.BAConvert(finalDem)
                val newBob = finalDem.toBytes()
                AmazonConnect.writeS3Object( resolutionStr + "/noobs", fileName, newBob)

                // finalDem.write("/Users/chrismangold/develop/preparedata/portsmouth/staging/landcover/out/" + "obstructGone.tif")


                obstructionStore = true

                processResult = true
              } catch {
                case e: IllegalArgumentException => {
                  println("No valid Bare Earth Received")
                  processResult = false
                }
              }


              val op = BuildBaseDemResponse( processResult.toString, bareearthStored.toString,
                firstreturnStored.toString, landcoverincluded.toString, firstreturnobstructionsincluded.toString,
                obstructionStore.toString)

              write(op)


            }
          }

        }
      }
    }
  }


  def tilePrepRoute =  {

    get {
      pathPrefix("tileprep") {

        path("raw") {
          parameters('src) {
            (src) => {

              complete {

                val sourcePath = src + "/sources"
                var fileList = AmazonConnect.getObjectsInBucket(sourcePath)

                for (name <- fileList) {

                  println(name._2)
                  if ( name._2.toLowerCase.contains( ".tif") ) {

                    val bytesToUse = AmazonConnect.getS3Object(name._2)

                    // Construct an object with instructions to fetch the raster
                    // Returns SingleBandGeoTiff
                    val gtIn = SinglebandGeoTiff(bytesToUse)

                    // Move from tile to rastersource
                    // Maintain Projections
                    val theRS = RasterSource(gtIn.tile, gtIn.extent)

                    // Generate Slope
                    val slopeRS = ElevationRasterSourceMethodExtensions(theRS).slope(0.00000898)

                    // Write out slope product
                    val lclTile = slopeRS.get
                    val lclExtent = slopeRS.rasterExtent.get.extent
                    val gtSlope = GeoTiff(lclTile, lclExtent, gtIn.crs)


                    val widthOfImage = gtSlope.raster.cols
                    val heightOfImage = gtSlope.raster.rows
                    //println( "Width " + widthOfImage )
                    //println( "height " + heightOfImage )

                    var numberRows:Int = (heightOfImage / 512).toInt

                    var numberCols:Int = (widthOfImage / 512).toInt

                    if ( heightOfImage % 512 > 0  ) {
                      numberRows += 1
                    }
                    if ( widthOfImage % 512 > 0  ) {
                      numberCols += 1
                    }

                    //println( "Rows " + numberRows )
                    //println( "Cols " + numberCols )

                    var rowStart:Int = 0
                    var rowEnd:Int = if ( numberRows > 0 ) 511 else gtSlope.extent.width.toInt


                    var heightCount = gtSlope.extent.height.toInt

                    for ( row <- 1 to numberRows ) {

                      var colStart:Int = 0
                      var colEnd:Int =  if ( numberCols > 0 ) 511 else gtSlope.extent.height.toInt

                      for ( col <- 1 to numberCols  ) {

                        var widthCount = gtSlope.extent.width.toInt
                        // println ( "Row " + row + " Col " + col + " RS " + rowStart.toInt + " CS " + colStart.toInt + " RE " + rowEnd.toInt + " CE " + colEnd.toInt)

                        val testTile = gtSlope.tile.crop(rowStart.toInt, colStart.toInt, rowEnd.toInt, colEnd.toInt)

                        val newExtent = gtSlope.raster.rasterExtent.extentFor(GridBounds(rowStart.toInt, colStart.toInt, rowEnd.toInt, colEnd.toInt))
                        val gtOut = GeoTiff(testTile, newExtent, gtSlope.crs)

                        var fileRootindex = name._2.lastIndexOf("/");
                        var origFileName = name._2.substring(fileRootindex + 1)
                        var extIndex = origFileName.lastIndexOf(".");

                        var ext = origFileName.substring(extIndex)
                        var rootName = origFileName.substring(0, extIndex - 1)
                        var outName = rootName + "_" + col + "_" + row +  ext

                        // Use when writing local
                        gtOut.write( "/Users/chrismangold/testSlopes/" + outName )


                        widthCount -= 512
                        colStart = colEnd + 1
                        colEnd =  if ( widthCount >= 512  ) colStart + 511 else gtSlope.extent.width.toInt

                      }

                      heightCount -= 512
                      rowStart = rowEnd + 1
                      rowEnd = if ( heightCount >= 512 )  rowStart + 511 else gtSlope.extent.height.toInt

                    }

                  }
                }
                "Done"
              }
            }
          }
        }
      }
    }
  }

  // Get tile from index server
  // http://localhost:8080/tile?jobid=<dem-src>&z=<z value>&x=<x value>&y=<y value>
  def gettile = path("tile") {

    get {
      parameters('jobid , 'z , 'x, 'y) {
        (jobid, z, x, y) => {

          respondWithMediaType(MediaTypes.`image/png`) {
            complete {

              println( jobid + " " + z + " "  + x + " " + y )

              val pngByteArray = SurgeIndexAPI.getZXYDocument (jobid.toString, z.toString, x.toString, y.toString)

              pngByteArray

            }
          }
        }
      }
    }
  }

  /**
   * This will be picked up by the runRoute(_) and used to intercept Exceptions
   */
  implicit def TutorialExceptionHandler(implicit log: LoggingContext) =
    ExceptionHandler {
      case e: Exception =>
        requestUri { uri =>
          complete(InternalServerError, s"Message: ${e.getMessage}\n Trace: ${e.getStackTrace.mkString("</br>")}" )
        }
    }


}


