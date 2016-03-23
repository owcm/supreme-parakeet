package tutorial

/**
 * Created by chrismangold on 1/8/16.
 */

import _root_.net.liftweb.json._
import com.typesafe.config.ConfigFactory
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiff}
import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import net.liftweb.json._
import net.liftweb.http.js._
import net.liftweb.http.js.JE._
import java.io._
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.params.HttpConnectionParams
import org.apache.http.util.EntityUtils
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpResponse}
import org.apache.http.client._
import org.apache.http.client.methods.{HttpPost, HttpGet}
import org.apache.http.impl.client.{HttpClientBuilder, DefaultHttpClient}
import scala.collection.mutable.StringBuilder
import scala.xml.XML
import org.apache.http.params.HttpConnectionParams
import org.apache.http.params.HttpParams
import org.codehaus.jackson.map.ObjectMapper
import scala.collection.mutable.StringBuilder
import scala.xml.XML
import scala.io.Source
import geotrellis.vector._
import com.typesafe.config._

object SurgeIndexAPI {

  // Construct Index url base

  val fqn = ConsulSupport.getCatalogHostIP()
  val baseUrl = "http://" + fqn + "/api/catalog/"

  println("Init Done")

  /**
   * Returns the text content from a REST URL. Returns a blank String if there
   * is a problem.
   */
  def getRestContent( url:String ): String = {

    val httpClient =  HttpClientBuilder.create().build()
    val httpResponse = httpClient.execute(new HttpGet(url))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()

      content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }

    return content
  }

  def getDocumentListFromPt( lat: String, lon: String, distanceMeters: String, propertyFilter: String): String = {

    val requestUrl = baseUrl + "search?props=bbox:@ptradius" + lat + "+" + lon + "+" +  distanceMeters
    println( requestUrl )
    val content = getRestContent(requestUrl)

    content

  }

  def postZXYDocument( uriStr: String,  pngBytes:Array[Byte] ) : Unit= {

    val postUrl = baseUrl + "tile/to/" + uriStr

    val httpClient: HttpClient = HttpClientBuilder.create().build(); //Use this instead

    try {

      val post = new HttpPost(postUrl)
      post.addHeader("Content-Type", "image/png")
      post.addHeader("Accept", "application/json")
      println(postUrl)
      //var jsonToSend = JsObj(("contentType", "image/png"), ("projection", "EPSG:3857"), ("bbox","test")).toJsCmd
      post.setEntity( new ByteArrayEntity(pngBytes));
      var response:HttpResponse = httpClient.execute(post);
      println(response)


    } catch {

      case e:Exception => println(e.getMessage)
    } finally
    {
      println("Data Posted " + postUrl )
    }

  }

  private def buildHttpClient(connectionTimeout: Int, socketTimeout: Int): DefaultHttpClient = {
    val httpClient = new DefaultHttpClient
    val httpParams = httpClient.getParams
    HttpConnectionParams.setConnectionTimeout(httpParams, connectionTimeout)
    HttpConnectionParams.setSoTimeout(httpParams, socketTimeout)
    httpClient.setParams(httpParams)
    httpClient
  }

  def getZXYDocument( uuid:String, z:String, x: String, y:String ) : Array[Byte]= {


    val gettUrl = baseUrl + "tile/for/" + uuid + "/" + z + "/" + x + "/" + y
    var returnVal = Array[Byte]()
    val httpClient: HttpClient = buildHttpClient(3000, 3000); //Use this instead

    try {

      println( "Reading " + gettUrl)
      val get = new HttpGet(gettUrl)
      get.addHeader("Content-Type", "image/png")
      val httpResponse = httpClient.execute( get )

      val entity = httpResponse.getEntity()
      if ( entity != null ) {

        returnVal = EntityUtils.toByteArray( entity )

        println("Packet length" + returnVal.length)
        println(returnVal)
        println( entity.getContent)

      }

    } catch {

      case e:Exception => println(e.getMessage)
    } finally
    {

    }

    returnVal

  }

  def getDocumentContent( urn: String, itemType: String, itemValue:String   ): String = {

    val requestUrl = baseUrl + "document/" + urn
    var uri = "empty"

    println( requestUrl )
    val content = getRestContent(requestUrl)

    val json = parse(content)

    val docItems = ( json \\ "document")

    val json2 = parse( docItems.values.toString)

    var myMap:Map[String,String] = Map()

    myMap += ("dataset" -> (json2 \\ "dataset").values.toString)
    myMap += ("type" -> (json2 \\ "type").values.toString)
    myMap += ("resolution" -> (json2 \\ "resolution").values.toString)
    myMap += ("filetype" -> (json2 \\ "filetype").values.toString)
    myMap += ("uri" -> (json2 \\ "uri").values.toString)

    val value = myMap( itemType)
    if (value.toString().toLowerCase == itemValue.toLowerCase()) {
      uri = myMap( "uri")
    }

    uri

  }


}
