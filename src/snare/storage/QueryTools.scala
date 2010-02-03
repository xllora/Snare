package snare.storage

import java.util.logging.Logger
import com.mongodb.{DB, DBCollection, BasicDBObject}
import java.util.UUID

/**
 * Basic querying mechanics
 *
 * @author Xavier Llora
 * @date Feb 2, 2010 at 10:59:33 PM
 * 
 */

object QueryTools {


  // Retrieve the heartbeats
  def queryHeartbeats (heartbeat:DBCollection,host:String,port:Int,pool:String,log:Logger)  = {
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
  def queryFetchRegisteredPeersInformation (sharedPool:DBCollection,host:String,port:Int,pool:String,log:Logger) = {
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
  def queryPeers (sharedPool:DBCollection,host:String,port:Int,pool:String,log:Logger) = {
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

  // Broadcast a message
  def queryBroadcast(message: BasicDBObject, sharedPool: DBCollection, db:DB, uuid:UUID, host: String, port: Int, pool: String,log:Logger) = {
    try {
      val cur = sharedPool.find
      val msg = new BasicDBObject
      msg.put("ts", System.currentTimeMillis)
      msg.put("msg", message)
      msg.put("source", uuid.toString)
      msg.put("type", "BROADCAST")
      var peers: List[String] = Nil
      while (cur.hasNext) {
        val peer = cur.next.get("_id").toString
        db.getCollection(peer).insert(msg)
        peers ::= peer
      }
      Some(peers)
    }
    catch {
      case e => log severe "[WebUI] Failed to connect with server " + host + " on port " + port + " for collection " + pool
      None
    }
  }

  // Notify the peer
  def queryNotifyPeer(uuid: String, message: BasicDBObject,  db:DB, sharedPool: DBCollection, host: String, port: Int, pool: String, log:Logger) = {
    try {
      val peer = new BasicDBObject
      peer.put("_id", uuid)
      if (sharedPool.find(peer).count == 1) {
        val msg = new BasicDBObject
        msg.put("ts", System.currentTimeMillis)
        msg.put("msg", message)
        msg.put("source", uuid.toString)
        msg.put("type", "DIRECT")
        db.getCollection(uuid).insert(msg)
        Some(uuid)
      }
      else
        None
    }
    catch {
      case e => log severe "[WebUI] Failed to connect with server " + host + " on port " + port + " for collection " + pool
      None
    }
  }

  // Fetch peer information
  def queryFetchPeerInformation(uuid: String, sharedPool: DBCollection, host: String, port: Int, pool: String,log:Logger) = {
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

}