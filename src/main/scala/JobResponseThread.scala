package tutorial

import java.io.BufferedReader
import java.io._

/**
 * Created by chrismangold on 1/18/16.
 */
class JobResponseThread extends Runnable {

  private var reader: BufferedReader = null

  private var name: String = null

  def this( is: InputStream, name: String) {
    this()
    this.reader = new BufferedReader(new InputStreamReader(is))
    this.name = name
  }

  def run {

    try {
      var line: String = reader.readLine
      while (line != null) {
        println(line)
        line = reader.readLine
      }
      reader.close
    }
    catch {
      case e: IOException => {
         println("IOError on job submit")
      }
    }
  }
}

