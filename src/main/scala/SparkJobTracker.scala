package tutorial

import org.apache.spark.launcher.SparkLauncher

/**
 * Created by chrismangold on 2/3/16.
 */
object SparkJobTracker {

  var SparkJobTracker_RUNNABLE = "RUNNABLE"
  var SparkJobTracker_SUCCESS = "SUCCESS"
  var SparkJobTracker_FAILED = "FAILED"
  var SparkJobTracker_STOPPED = "STOPPED"


  var sparkJobs:Map[ String , ( Thread, Process, String)] = Map()

  def addJobToTrack( jobId : String, sparkThread:Thread, sparkHandler: Process, status:String): Unit = {

    sparkJobs += (  jobId -> ( sparkThread, sparkHandler, SparkJobTracker_RUNNABLE  ) )

  }

  def removeJobToTrack( jobId : String ): Unit = {

    sparkJobs -= ( jobId )

  }

  def getJobStatus( jobId : String ) : String = {

    var statusStr = SparkJobTracker_FAILED

    if ( sparkJobs.contains( jobId ) ) {

      val sparkTuble = sparkJobs.getOrElse( jobId, null)


      if ( sparkTuble._3 != null )
        statusStr = sparkTuble._3


    }

    statusStr

  }

  def setJobStatus( jobId : String, status: String ) = {


    if ( sparkJobs.contains( jobId ) ) {

      val sparkTuble = sparkJobs.getOrElse( jobId, null)

      val newTuble = ( sparkTuble._1, sparkTuble._2, status )
      sparkJobs = sparkJobs + ( jobId -> newTuble )

    }

  }


  def killSparkJob( jobId : String ) : String = {

    var statusStr = SparkJobTracker_STOPPED

    if ( sparkJobs.contains( jobId ) ) {

      val sparkTuble = sparkJobs.getOrElse( jobId, null)

      if ( sparkTuble._2 != null ) {
        sparkTuble._2.destroy()
        sparkJobs -= jobId
      }

    }

    statusStr

  }

}
