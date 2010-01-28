package snare.tools

import com.mongodb.BasicDBObject
import com.mongodb.util.JSON

/**
 * A collection of implicit conversion methods
 *
 * @author Xavier Llora
 * @date Jan 27, 2010 at 10:31:38 PM
 * 
 */
object Implicits {

  implicit def String2BasicDBObject(s:String):BasicDBObject = JSON.parse(s).asInstanceOf[BasicDBObject]

  implicit def BasicDBObject2String(bson:BasicDBObject):String = bson.toString

}