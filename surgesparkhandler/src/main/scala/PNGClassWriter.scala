package surgesparkhandler

/**
 * Created by chrismangold on 3/20/16.
 */

import geotrellis.raster.Tile
import geotrellis.spark.io.avro.AvroEncoder
import geotrellis.spark.io.index.KeyIndex
import geotrellis.spark.{TileLayerMetadata, SpatialKey, Metadata, LayerId}
import geotrellis.spark.io.file.FileAttributeStore
import geotrellis.util.Filesystem
import org.apache.spark.rdd.RDD

/**
 * Created by chrismangold on 3/18/16.
 * **/
class PNGClassWriter(

                      val operation:String,
                    val jobId: String
  ) {

  //def uuid = java.util.UUID.randomUUID.toString

  def write ( zoom: Int, rdd: RDD[(SpatialKey, Array[Byte])], rm: TileLayerMetadata[SpatialKey], jobId:String): Unit = {


    rdd.foreach( f=> {

      val uriStr = jobId + "/" + zoom.toString + "/" + f._1._1.toString + "/" + f._1._2.toString
      println( "String is : " + uriStr + " number of bytes " + f._2.length)

      SurgeIndexAPI.postZXYDocument( uriStr, f._2 )

    })


  }

}


object PNGClassWriter {

  def apply( operation: String, job: String): PNGClassWriter =

    new PNGClassWriter(
      operation,
      job
    )

}