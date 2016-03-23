package tutorial

/**
 * Created by chrismangold on 1/10/16.
 */

import java.io.File

object LocalFileAccess {


  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

}
