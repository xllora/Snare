package snare.storage

import java.util.logging.Logger
import java.util.UUID
import com.mongodb.{Mongo, DB, DBCollection, BasicDBObject}

/**
 * Basic querying mechanics
 *
 * @author Xavier Llora
 * @date Feb 2, 2010 at 10:59:33 PM
 * 
 */

class QueryTools ( pool: String, host: String, port: Int, log:Logger) {

  val DEFAULT_DB = "Snare"
  val HEARTBEAT_COLLECTION = "heartbeat"
  val POOL_COLLECTION = "pool"

  //
  // Main heartbeat collection
  //
  protected val m = new Mongo
  protected val db = m getDB DEFAULT_DB
  protected val heartbeat = db getCollection HEARTBEAT_COLLECTION + "_" + pool
  protected val sharedPool = db getCollection POOL_COLLECTION + "_" + pool

  //
  // Authentication
  //
  def authenticate(user:String,pswd:String) = db.authenticate(user,pswd.toCharArray)

  // Retrieve the heartbeats
  def queryHeartbeats  = {
    try {
      var res:List[BasicDBObject] = Nil
      var cur = heartbeat.find
      while ( cur.hasNext )
        res ::= cur.next.asInstanceOf[BasicDBObject]
      Some(res)
    }
    catch {
      case e => log  severe "[WebUI] Failed to connect with server "+host+" on port "+port+" for collection "+pool
                None
    }
  }

  // Retrieve the peers information
  def queryFetchRegisteredPeersInformation = {
    try {
      var res: List[BasicDBObject] = Nil
      val cur = sharedPool.find
      while (cur.hasNext) res ::= cur.next.asInstanceOf[BasicDBObject]
      Some(res)
    }
    catch {
      case e => log  severe "[WebUI] Failed to connect with server "+host+" on port "+port+" for collection "+pool
                None
    }
  }

  // Retrieve the query peers
  def queryPeers = {
    try {
      var res: List[String] = Nil
      val cur = sharedPool.find
      while (cur.hasNext) res ::= cur.next.get("_id").toString
      Some(res)
    }
    catch {
      case e => log severe "[WebUI] Failed to connect with server " + host + " on port " + port + " for collection " + pool
      None
    }
  }

  // Fetch peer information
  def queryFetchPeerInformation(uuid: String) = {
    try {
      val peer = new BasicDBObject
      peer.put("_id", uuid)
      if (sharedPool.find(peer).count == 1)
        Some(sharedPool.findOne(peer).asInstanceOf[BasicDBObject])
      else
        None
    }
    catch {
      case e => log severe "[WebUI] Failed to connect with server " + host + " on port " + port + " for collection " + pool
      None
    }
  }

  def queryPendingNotifications (uuid:String) = {
    try {
      val peer = db getCollection uuid
      Some(peer.find.count)
    }
    catch {
      case e => log severe "[WebUI] Failed to connect with server " + host + " on port " + port + " for collection " + pool
      None
    }
  }

}

/**
 * Companion object for querying mechanics
 *
 * @author Xavier Llora
 * @date Feb 2, 2010 at 10:59:33 PM
 *
 */

object QueryTools {

  def apply (pool: String, host: String, port: Int, log:Logger) = new QueryTools(pool,host,port,log) 

}

