package tutorial

import java.io.{IOException, InputStreamReader, InputStream, BufferedReader}
import com.typesafe.config.ConfigFactory
import org.apache.spark.launcher.SparkLauncher

/**
 * Created by chrismangold on 1/22/16.
 */
class SparkSubmitThread  extends Runnable{

  private var applicationName: String = null
  private var jsonStr: String = null
  private var jobId : String = null

  def this( jobid:String , applicationname: String, json: String ) {

    this()
    this.jobId = jobid
    this.applicationName = applicationname
    this.jsonStr = json
  }

  def run {

    try {

      val config = ConfigFactory.load()
      val hdfsPort = config.getString("hdfs.port")

      val masterName = ConsulSupport.getEMRHostIP()

      val hdfsUrl = "hdfs://" + masterName + ":" + hdfsPort + "/user/spark/share/lib/simple-download_2.10-0.1-SNAPSHOT.jar"
      print (hdfsUrl)

      val spark = new SparkLauncher( )
      .setAppResource( hdfsUrl )
      .setMainClass("surgesparkhandler.RequestHandler")
      .setVerbose( true )
      .setAppName( applicationName )
      // Docker should set SPARK_HOME
        .setSparkHome("/Users/chrismangold/develop/spark/spark-1.5.2/")
        .setMaster("yarn-cluster")
        .setDeployMode("cluster")
        .setConf(SparkLauncher.DRIVER_MEMORY, "10g")
        .setConf(SparkLauncher.EXECUTOR_MEMORY, "20g")
        .setConf(SparkLauncher.EXECUTOR_CORES, "4")
        .setConf("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .setConf("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .setConf("spark.kryoserializer.buffer.max", "512m")
        .setConf("spark.akka.frameSize","1000")
        .setConf("spark.akka.timeout",	"1000")
        .setConf("spark.akka.heartbeat.pauses",	"60000")
        .setConf("spark.akka.heartbeat.interval","10000")
        .setConf("spark.default.parallelism","6")
        .setConf("spark.rdd.compress","true")
        .setConf("spark.shuffle.service.enabled","true")
        .setConf("spark.dynamicAllocation.enabled", "true")
        .setConf("spark.dynamicAllocation.minExecutors","5")
        .setConf("spark.default.parallelism","20")
        .setConf("spark.yarn.executor.memoryOverhead","1024")
        .setConf("spark.yarn.driver.memoryOverhead","1024")
        .setConf("spark.yarn.jar","hdfs:///user/spark/share/lib/surgesparkhandler-assembly-0.1-SNAPSHOT.jar")
        .addAppArgs( jsonStr )
        .launch();

      println("Spark Submit job for " + applicationName)
      val inputStreamReaderRunnable:JobResponseThread = new JobResponseThread(spark.getInputStream(), "input");
      val inputThread:Thread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
      inputThread.start();

      val errorStreamReaderRunnable:JobResponseThread = new JobResponseThread(spark.getErrorStream(), "error");
      val errorThread:Thread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
      errorThread.start();

      SparkJobTracker.addJobToTrack( jobId, inputThread, spark, SparkJobTracker.SparkJobTracker_RUNNABLE)

      println("Waiting for reply...");
      val exitCode:Int = spark.waitFor();

      if ( exitCode == 0 ) {
        // Job was a success
        SparkJobTracker.setJobStatus( jobId, SparkJobTracker.SparkJobTracker_SUCCESS)
      } else {
        // Job was a failure
        SparkJobTracker.setJobStatus( jobId, SparkJobTracker.SparkJobTracker_FAILED)
      }
      System.out.println("Finished! Exit code:" + exitCode);

    }
    catch {
      case e: IOException => {
        println("IOError on job submit")
      }
    }
  }
}

