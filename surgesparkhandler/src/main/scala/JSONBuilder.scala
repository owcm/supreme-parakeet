package surgesparkhandler

import geotrellis.raster.RasterExtent
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST._
import net.liftweb.json.Extraction._
import net.liftweb.json.Printer._

/**
 * Created by chrismangold on 1/22/16.
 */
object JSONBuilder {

  implicit val formats = DefaultFormats

  case class BboxBody( `@WKT`: String)
  case class JobBody( dataset: String, jobid: String, resolution: String, bbox:BboxBody,
                           urlRoot:String, result:String, jobtype: String,
                           projection: String, `contentType`: String)

  case class OriginBody( `@WKT`: String)
  case class CenterBody( `@WKT`: String)
  case class UploadDocument ( origin: OriginBody, dataset: String, uri: String, directory: String,
                              center: CenterBody, location: String, bbox: BboxBody, filetype: String,
                              `type`: String, filename: String, resolution: String )


  def buildJobJSON ( jobid: String, op: String,  cellRes: String, LLLat:Float,
                     LLLon:Float, URLLat:Float, URLon:Float, result:String  ): String = {

     if ( cellRes.length > 0  ) {

       val rootString = "/api/catalog/tile/for/" + jobid
       val wktString = "POLYGON ((" + LLLon + " " + LLLat + "," + LLLon + " " + URLLat + "," +
         URLon + " " + URLLat + "," + URLon + " " + LLLat + "," + LLLon + " " + LLLat + "))"

       val BboxBody = new BboxBody(wktString)
       val documentContent = new JobBody(jobid, jobid, cellRes, BboxBody, rootString, result, op,
         "EPSG:3857", "image/png")
       pretty(render(decompose( documentContent )))

     } else {

       val BboxBody = new BboxBody("Unknown")
       val documentContent = new JobBody(jobid, jobid, "Unknown", BboxBody, "Unknown", result, op, "EPSG:3857", "image/png")
       pretty(render(decompose( documentContent )))

     }


  }

  def buildDocumentJSON (  srcType: String, fileName: String,  resolution:String, re: RasterExtent ): String = {

    re.extent.southWest
    re.extent.northEast

    val origin = "POINT (" +  re.extent.center.x.toString + " " + re.extent.center.y.toString + ")"
    var originBody = new OriginBody( origin )

    val center = "POINT (" +  re.extent.center.x.toString + " " + re.extent.center.y.toString + ")"
    var centerBody = new CenterBody( center )

    val uri = "hdfs://" + srcType + "/" + resolution + "/" + fileName

    val wktString = "POLYGON ((" + re.extent.southWest.x + " " + re.extent.southWest.y + "," +
      re.extent.southWest.x + " " + re.extent.northEast.y + "," +
      re.extent.northEast.x + " " + re.extent.northEast.y + "," +
      re.extent.northEast.x + " " +re.extent.southWest.y + "," + re.extent.southWest.x + " " + re.extent.southWest.y + "))"

    val BboxBody = new BboxBody(wktString)

    val documentContent = new UploadDocument(originBody,
      resolution, uri, resolution, centerBody, "HDFS", BboxBody, "tif", srcType, fileName, resolution )
    pretty(render(decompose( documentContent )))

  }

}