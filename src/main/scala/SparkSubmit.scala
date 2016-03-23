/**
 * Created by chrismangold on 1/14/16.
 */
package tutorial

import java.io.{FileInputStream, InputStream}
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.launcher.SparkLauncher ;
import java.io.FileNotFoundException;
import java.io.FileInputStream ;
import java.io.InputStream ;
import java.util.concurrent.TimeUnit ;
import java.io.IOException
import java.nio.file.{Paths, Files}
import scala.tools.nsc.io._
import scala.io.Source
import com.typesafe.config._
import scala.collection.immutable.StringOps
import org.apache.commons.io.FileUtils;
import java.io.File
import collection.JavaConversions._

/**
 * Created by chrismangold on 12/16/15.
 */

// http://spark.apache.org/docs/1.5.2/api/java/index.html?org/apache/spark/launcher/package-summary.html
// https://www.codatlas.com/github.com/apache/spark/HEAD/launcher/src/main/java/org/apache/spark/launcher/SparkLauncher.java?line=77


object SparkSubmit {

  val initFileName = "initrequire.txt"
  val config = ConfigFactory.load()
  val hdfsPort = config.getString("hdfs.port")

  def submitJob( jobId: String, applicationName: String, json: String ): Unit = {

    if ( ifInitRequiredExists )
      deployUpdateConfig()

    val submitRunnable:SparkSubmitThread = new SparkSubmitThread( jobId, applicationName, json );
    val sparkThread:Thread = new Thread( submitRunnable, "SparkSubmit Job input");

    sparkThread.start();


  }

  def ifInitRequiredExists (): Boolean  = {

    Files.exists(Paths.get( initFileName ))

  }

  def createInitRequired(): Unit = {

    val string = "Init config files"
    val file = new File("initrequire.txt")
    FileUtils.writeStringToFile(file, string)
  }

  def deployUpdateConfig (): Unit  = {

    val cFiles:String = config.getString("hadoop-config-files")
    val clFiles = cFiles.split(',')
    val masterName = ConsulSupport.getEMRHostIP()

    val hadoopHome = sys.env("HADOOP_CONF_DIR") + "/"
    // Replace IPs
    for ( file <- clFiles ) {
      modifyFile(  file, "deploy/" + file, hadoopHome + file, "REPLACEIP", masterName)
    }

    // Replace PORT
    for ( file <- clFiles ) {
      modifyFile(  file, hadoopHome  + file, hadoopHome + file, "REPLACEPORT", hdfsPort)
    }

    // Delete directory since files have been deployed
    FileUtils.deleteQuietly(new File(initFileName) )
  }

  def modifyFile(newFileName: String, sourceFilePath: String, targetFilePath:String, oldValue: String, newValue: String ) {
    scala.tools.nsc.io.File(targetFilePath).printlnAll(
      Source.fromFile(sourceFilePath).getLines().map {
        _.replaceAll(oldValue, newValue)
      }.toSeq:_*)
  }

}