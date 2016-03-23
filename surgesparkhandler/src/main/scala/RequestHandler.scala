package surgesparkhandler

/**
 * Created by chrismangold on 12/2/15.
 */

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext

object RequestHandler {

  val config = ConfigFactory.load()


  // Setup default color breaks for op

  def main(args: Array[String]) {

    System.setProperty("HADOOP_USER_NAME", "root")

    println( "Report values passed in")
    for(i <- 0 until args.length){
      println("i is: " + i);
      println("i'th element is: " + args(i));
    }

    val spark = new SparkContext()

    try {

      val jsonCmdString = args(0)

      run( spark, jsonCmdString )
     } finally {

    }

    def run( implicit sc: SparkContext, jsonCmdString: String ) = {

      // TODO Will find file from lookup in catalog index hardcoded for now

      SurgeOpParse.parseCmd(jsonCmdString)

      val opType = SurgeOpParse.getProperty("optype")
      var errcode:Int = 0
      if ( opType != "analytic") {
        errcode = SurgePipelines.Process( sc,jsonCmdString )
      } else {
        errcode = SurgeAnalytics.Process( sc,jsonCmdString )
      }

      if ( errcode != 0) {
        // Throw exception to foward earro back to calling sources
        throw new Exception("Failure in operation")
      }
    }


  }

}
