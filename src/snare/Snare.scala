package snare

import logger.LoggerFactory
import scala.concurrent.ops.spawn
import com.mongodb.{BasicDBObject, Mongo}
import snare.tools.UUIDIdentitity._

/**
 * The Snare main class
 *
 * @author Xavier Llora
 * @date Jan 25, 2010 at 7:30:48 PM
 *
 */

class Snare(val name: String, val pool: String, val host: String, val port: Int,
            notify: (BasicDBObject) => Boolean) {
  val log = LoggerFactory.getLogger
  val DEFAULT_DB = "Snare"
  val HEARTBEAT_COLLECTION = "heartbeat"
  val POOL_COLLECTION = "pool"
  var HEARTBEAT_INTERVAL = 3000
  var EVENT_LOOP_INTERVAL = 3000
  val uuid = uniqueUUID(name)
  protected val ucnd = new BasicDBObject;
  ucnd.put("_id", uuid.toString)

  //
  // Basic heartbeat control of the heartbeat
  //
  // TODO Should the heartbeat and event loop be merged?
  protected var activityHeartbeat = false

  def heartbeat = activityHeartbeat

  def heartbeat_=(stat: Boolean) = stat match {
  // Need to get started
    case true if !activityHeartbeat => {
      activityHeartbeat = true; runHeartbeat; true
      // TODO: Update heartbeat status on the pool collection
    }
    // Already running
    case true if activityHeartbeat => true
    // Already not running
    case false if !activityHeartbeat => false
    // Need to update activity to stop
    case false if activityHeartbeat => {
      activityHeartbeat = false; false
      // TODO: Update heartbeat status on the pool collection
    }
  }


  //
  // Basic notification event loop
  //
  // TODO Should the heartbeat and event loop be merged?
  protected var activityEventLoop = false

  def notifications = activityEventLoop

  def notifications_=(stat: Boolean) = stat match {
    // Need to get started
    case true if !activityEventLoop => {
      activityEventLoop = true; runEventLoop; true
      // TODO: Update event loop status on the pool collection
    }
    // Already running
    case true if activityEventLoop => true
    // Already not running
    case false if !activityEventLoop => false
    // Need to update activity to stop
    case false if activityEventLoop => {
      activityEventLoop = false; false
      // TODO: Update event loop status on the pool collection
    }
  }

  //
  // Main heartbeat collection
  //
  protected val m = new Mongo
  protected val db = m getDB DEFAULT_DB
  protected val activity = db getCollection HEARTBEAT_COLLECTION
  protected val sharedPool = db getCollection POOL_COLLECTION + "_" + pool
  protected val instance = db getCollection uuid.toString

  protected val ID = new BasicDBObject
  ID.put("_id", uuid.toString)
  protected val createdAt = System.currentTimeMillis

  // The shutdown hook for this instance
  private val sdh = new SnareShutdownHook(this)

  protected def registerToPool = {
    // TODO This is tight to the heartbeat. What about the event loop?
    val nc = networkConfiguration
    nc.put("ts", System.currentTimeMillis)
    val entry = new BasicDBObject
    entry.put("_id",uuid.toString)
    entry.put("ts",createdAt)
    entry.put("network",nc)
    sharedPool.update(ID, entry, true, false)
    Runtime.getRuntime.addShutdownHook(sdh)
    this
  }

  protected[snare] def unregisterFromPool = {
   // TODO This is tight to the heartbeat. What about the event loop?
   try {
      activity.remove(ID)
      sharedPool.remove(ID)
      instance.drop
    }
    catch {
      case _ =>
    }
    finally {
      Runtime.getRuntime.removeShutdownHook(sdh)
    }
    this
  }

  //
  // The thread that runs the heart beat
  //
  protected def runHeartbeat = {
    //
    // The heart beat
    //
    spawn {
      registerToPool
      log info "[HRTB] Heartbeat engaged for "+uuid
      while (activityHeartbeat) {
        Thread.sleep(HEARTBEAT_INTERVAL)
        val update = new BasicDBObject
        val value = new BasicDBObject
        value.put("ts", System.currentTimeMillis)
        update.put("$set", value)
        try {
          // Activity update
          activity.update(ucnd, update, true, false)
          log finest "[SUCC] " + uuid + " " + update
        }
        catch {
          case e => log warning "[FAIL] " + uuid + " " + e.getCause
        }
      }
      log info "[HRTB] Heartbeat disengaged for "+uuid
      unregisterFromPool
    }
  }

  def runEventLoop = {
    //
    // The notification threat
    //
    spawn {
      log info "[EVTL] Eveent loop engaged for " + uuid
      while (activityEventLoop) {
        val cur = instance.find
        while (cur.hasNext) {
          Thread.sleep(EVENT_LOOP_INTERVAL)
          val msg = cur.next.asInstanceOf[BasicDBObject]
          if ( notify(msg) )
            log info "[EVTL] Notification processed by "+uuid+" "+msg
          else
            log info "[EVTL] Notification ignored by "+uuid+" "+msg
          instance.remove(msg)
        }
      }
      log info "[EVTL] Eveent loop disengaged for " + uuid
    }
  }

  //
  // Returns the peers in the pool
  //
  def peers = {
    var res: List[String] = Nil
    val cur = sharedPool.find
    while (cur.hasNext) res ::= cur.next.get("_id").toString
    res
  }

  def broadcast(message: BasicDBObject) = {
    val cur = sharedPool.find
    val msg = new BasicDBObject
    msg.put("ts", System.currentTimeMillis)
    msg.put("msg", message)
    msg.put("source", uuid.toString)
    msg.put("type","BROADCAST")
    var peers: List[String] = Nil
    while (cur.hasNext) {
      val peer = cur.next.get("_id").toString
      db.getCollection(peer).insert(msg)
      peers ::= peer
    }
    peers
  }

  def notifyPeer(uuid: String, message: BasicDBObject) = {
    val peer = new BasicDBObject
    peer.put("_id", uuid)
    if (sharedPool.find(peer).count == 1) {
      val msg = new BasicDBObject
      msg.put("ts", System.currentTimeMillis)
      msg.put("msg", message)
      msg.put("source",uuid.toString)
      msg.put("type","DIRECT")
      db.getCollection(uuid).insert(msg)
      Some(uuid)
    }
    else
      None
  }

}

/**
 * Snare companion object
 *
 * @author Xavier Llora
 * @date Jan 25, 2010 at 7:30:48 PM
 *
 */
object Snare {
  def apply(name: String, pool: String, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, "localhost", 27017, notify)

  def apply(name: String, pool: String, port: Int, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, "localhost", port, notify)

  def apply(name: String, pool: String, host: String, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, host, 27017, notify)

  def apply(name: String, pool: String, host: String, port: Int, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, host, port, notify)

  def unapply(s: Snare): Option[(String, String, String, Int)] = Some((s.name, s.pool, s.host, s.port))
}