package tutorial

/**
 * Created by chrismangold on 12/18/15.
 */
import com.typesafe.config._
import net.liftweb.json._
import java.io._


object ConsulSupport {


  implicit val formats = DefaultFormats

  case class AccumuloParts( ID: String, Service: String, Tags: List[String], Address: String, Port: Int )
  case class Accumulo( accumulo: AccumuloParts )
  case class Response( Node: Map[String, String], Services:Accumulo )
  case class IndexResponse( Node: String, Address: String, ServiceID: String,ServiceName: String,
                            ServiceTags: String, ServiceAddress: String, ServicePort: Int  )

  def getEMRHostIP (): String  = {

    // Load in consul service data
    val config = ConfigFactory.load()
    val hostName = config.getString("consul-name")

    val url = "http://" + hostName + ":8500/v1/catalog/node/emrmaster"
    val result = scala.io.Source.fromURL(url).mkString

    val jsonObject = parse( result )
    val pr:Response = jsonObject.extract[Response]

   // val nodeEntry = pr.Node
   // val nodeMap:Map[String, String ] = nodeEntry.toMap
    val tags = pr.Services.accumulo.Tags
    val newName = tags(1)
    println( newName )

    newName

  }

  def getCatalogHostIP (): String  = {

    // Load in consul service data
    val hostName = ConfigFactory.load().getString("consul-name")
    println( hostName)
    val url = "http://" + hostName + ":8500/v1/catalog/service/catalog"
    println( url)
    val result = scala.io.Source.fromURL(url).mkString
    println(result)

    val jsonObject = parse( result )
    val pr:IndexResponse = jsonObject.extract[IndexResponse]

    val ip = pr.Address
    val port = pr.ServicePort.toString

    val fqn = ip + ":" + port
    println( "Catalog Link: " + fqn )
    fqn

  }

}
