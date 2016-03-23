/**
 * Created by chrismangold on 11/3/15.
 */
package surgesparkhandler

import com.amazonaws ._
import com.amazonaws.auth._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import com.typesafe.config._

import java.io ._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Try}

// API
// http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html

object AmazonConnect {


  // Based on application from: http://www.mydbapool.com/a-simple-program-to-scala-working-with-amazon-s3/
  val config =  ConfigFactory.load()
  val bucket = config.getString( "aws.s3.bucket")

  var client: AmazonS3Client = null

  try {

    val providerChain = new DefaultAWSCredentialsProviderChain()
    client = new AmazonS3Client(  providerChain )

  } catch {
    case ex: Exception => println( "Failed on credentials request:" + ex.getMessage())
  }

  // Check if file exists is S3
  def fileIsUploadedToS3( bucketToUse: String, uploadPath: String): Boolean = {

    try {
      client.getObjectMetadata(bucketToUse, uploadPath)
      true
    } catch {
      case ex: Exception => println(ex.getMessage()); false
    }

  }

  // Download file to target location
  // srcPath - name of file is S3 bucket
  // downloadPath - fully qualified path to disk with file name to store file.
  def downloadFromS3( uploadPath: String, downloadPath: String) {


    if (!fileIsUploadedToS3( bucket.toString, uploadPath)) {
      throw new RuntimeException( s"cannot download")
    }

    // Start timer to calculated download time
    val timestampStart: Long = System.currentTimeMillis / 1000
    client.getObject( new GetObjectRequest(bucket.toString, uploadPath), new File(downloadPath))
    val timestampStop: Long = System.currentTimeMillis / 1000
    val downLoadTime = timestampStop - timestampStart ;

    println( "Took " + downLoadTime + " seconds to download " + uploadPath )

  }

  // Method to support discovery of objects in bucket
  def getObjectsInBucket( demPrefix: String ) : ListBuffer[(Int,String)] = {


    println( "in" )
    // Start timer to calculated download time
    val timestampStart: Long = System.currentTimeMillis / 1000

    val fileList = ListBuffer[(Int,String)]()
    var counter:Int = 0

    // From
    // https://github.com/Atigeo/spark-job-rest/blob/master/examples/s3-download-job/src/main/scala/com.job/S3Utils.scala

    val listObjectsRequest = new ListObjectsRequest()
      .withBucketName(bucket.toString).withPrefix(demPrefix)

    var objectListing: ObjectListing = null

    do {

      import scala.collection.JavaConversions._

      objectListing = client.listObjects(listObjectsRequest)
      objectListing.getObjectSummaries.foreach { objectSummary =>
        if(!objectSummary.getKey.endsWith( ",")) {
          println(objectSummary.getKey.toString)
          fileList += Tuple2(counter, objectSummary.getKey)
          counter = counter + 1
        }
      }
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    } while (objectListing.isTruncated())

    fileList

  }

  // Convert inputstreams to bytearray
  def inputStreamToByteArray(is: InputStream): Array[Byte] =
    Iterator continually is.read takeWhile (-1 !=) map (_.toByte) toArray

  // Pull object from S3 as inputstream and return byte array
  // Used to read geotiffs from S3
  def getS3Object( bucketToUse:String, uploadPath: String) : Array[Byte] = {


  //  if (!fileIsUploadedToS3( bucket, uploadPath)) {
    //  throw new RuntimeException( s"cannot download")
   // }

    // Start timer to calculated download time
    val timestampStart: Long = System.currentTimeMillis / 1000
    val s3Object = client.getObject( new GetObjectRequest(bucketToUse, uploadPath))
    val timestampStop: Long = System.currentTimeMillis / 1000
    val downLoadTime = timestampStop - timestampStart ;

    println( "Took " + downLoadTime + " seconds to download " + uploadPath )

    var arrayOfBytes = inputStreamToByteArray( s3Object.getObjectContent )

    // Need to close the S3 object input stream or we eventually fail
    s3Object.getObjectContent.close()

    // Return an array of bytes
    arrayOfBytes

  }

}
