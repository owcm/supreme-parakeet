package tutorial

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import com.typesafe.config._

object Main extends App {

    // Load service connection points
    val config = ConfigFactory.load()
    val host = config.getString("http.host")
    val port = config.getInt("http.port")

    // we need an ActorSystem to host our service
    implicit val system = ActorSystem()

    //create our service actor
    val service = system.actorOf(Props[GeoTrellisServiceActor], "geotrellis-service")

    // Flag that the system has been restarted
    SparkSubmit.createInitRequired()
    //bind our actor to HTTP
    IO(Http) ! Http.Bind(service, interface = host, port = port)

}
