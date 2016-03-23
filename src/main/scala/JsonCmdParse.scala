/**
 * Created by chrismangold on 2/11/16.
 */
package tutorial

import spray.httpx.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.json._

/**
 * Created by chrismangold on 2/3/16.
 */

case class JobKill( jobId: String, status:String )
case class JobStatus( jobId: String, status:String )
case class HLZHelicopterparms( diameter:String, approach:String, dimension:String, degrees: String, obstacleRatio: String, `type`: String)
case class HLZPost( extent: List[Double], helicopters: List[HLZHelicopterparms] )
case class TPIOpparms( cellsize:String)
case class TPIOpPost( extent: List[Double], parms: TPIOpparms)
case class GenericOpPost( extent: List[Double] )
case class ViewOpparms( tgtHgt:String, orgHgt:String )
case class ViewShedPost( pt: List[Double], parms: ViewOpparms )
case class DemBasePost( landcoverfilepath:String, bareearthfilepath:String, firstreturnfilepath:String, resolution:String  )
case class UploadPost( `type`: String, location: String )
case class BuildObsPost( bareearthlocation: String, firstreturnlocation: String )
case class BuildHoverPost( bareearthlocation: String, firstreturnlocation: String )
case class BuildObsLCPost( srcdem: String, srclandcover: String )



object JsonCmdParse extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val HLZHelicopterParmsFormats = jsonFormat6 ( HLZHelicopterparms )
  implicit val HLZPortofolioFormats = jsonFormat2( HLZPost )
  implicit val ObsDemBaseFormats = jsonFormat4( DemBasePost )
  implicit val ViewOpparmsPortofolioFormats = jsonFormat2 ( ViewOpparms )
  implicit val ViewShedPostPortofolioFormats = jsonFormat2( ViewShedPost )
  implicit val TPIparmsPortofolioFormats = jsonFormat1 ( TPIOpparms )
  implicit val TPIPostPortofolioFormats = jsonFormat2( TPIOpPost )
  implicit val GenericPortofolioFormats = jsonFormat1( GenericOpPost )
  implicit val UploadPostPortofolioFormats = jsonFormat2( UploadPost )
  implicit val BuildObsPostPortofolioFormats = jsonFormat2( BuildObsPost )
  implicit val BuildObsLCPostFormats = jsonFormat2( BuildObsLCPost )
  implicit val BuildHoverPostFormats = jsonFormat2( BuildHoverPost )


}
