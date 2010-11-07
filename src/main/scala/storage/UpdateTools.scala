package snare.storage

import java.util.logging.Logger
import com.mongodb.BasicDBObject
import java.util.UUID
import snare.tools.UUIDIdentitity._

/**
 * uda
 *
 * @author Xavier Llora
 * @date Feb 3, 2010 at 8:23:26 AM
 * 
 */

class UpdateTools ( uuid:UUID, name:String, pool: String, createdAt:Long, host: String, port: Int, log:Logger )
extends QueryTools (pool,host,port,log) {


  protected val ucnd = new BasicDBObject;
  ucnd.put("_id", uuid.toString)

  val instance = db getCollection uuid.toString

  // Register to a pool
  def registerToPool(ID: BasicDBObject, metadata:BasicDBObject) = {
    val nc = networkConfiguration
    nc.put("ts", System.currentTimeMillis)
    val entry = new BasicDBObject
    entry.put("_id", uuid.toString)
    entry.put("ts", createdAt)
    entry.put("name", name)
    entry.put("pool", pool)
    entry.put("metadata", metadata)
    entry.put("interfaces", nc)
    sharedPool.update(ID, entry, true, false)
    updateHeartbeat(createdAt)
    this
  }

  // Unregister from a pool
  def unregisterFromPool(ID:BasicDBObject) = {
   try {
      heartbeat.remove(ID)
      sharedPool.remove(ID)
      instance.drop
    }
    catch {
      case _ =>
    }
    this
  }

  // Update the heartbeat
  def updateHeartbeat = {
    val update = new BasicDBObject
    val value = new BasicDBObject
    value.put("ts", System.currentTimeMillis)
    update.put("$set", value)

    // Activity update
    heartbeat.update(ucnd, update, true, false)
  }

  // Update the heartbeat and creation time
  protected def updateHeartbeat(createTS:Long) = {
    val update = new BasicDBObject
    val value = new BasicDBObject
    value.put("createdAt",createTS)
    value.put("ts", System.currentTimeMillis)
    update.put("$set", value)

    // Activity update
    heartbeat.update(ucnd, update, true, false)
  }

    // Broadcast a message
  def queryBroadcast(message: BasicDBObject) = {
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
  def queryNotifyPeer(peerUUID:String,message: BasicDBObject) = {
    try {
      val peer = new BasicDBObject
      peer.put("_id", peerUUID)
      if (sharedPool.find(peer).count == 1) {
        val msg = new BasicDBObject
        msg.put("ts", System.currentTimeMillis)
        msg.put("msg", message)
        msg.put("source", uuid.toString)
        msg.put("type", "DIRECT")
        db.getCollection(peerUUID).insert(msg)
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
}


/**
 * Companion object for update mechanics
 *
 * @author Xavier Llora
 * @date Feb 2, 2010 at 10:59:33 PM
 *
 */

object UpdateTools {

  def apply ( uuid:UUID, name:String, pool: String, createdAt:Long, host: String, port: Int, log:Logger)  = new UpdateTools(uuid,name,pool,createdAt,host,port,log)

}
