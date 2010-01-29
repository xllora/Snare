package snare

import logger.LoggerFactory
import scala.concurrent.ops.spawn
import com.mongodb.{BasicDBObject, Mongo}
import snare.tools.UUIDIdentitity._
import java.util.UUID
import snare.tools.Implicits._

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
  var EVENT_LOOP_INTERVAL = 6000
  val uuid = uniqueUUID(name)
  protected val ucnd = new BasicDBObject;
  ucnd.put("_id", uuid.toString)

  //
  // Basic heartbeat control of the heartbeat
  //
  private var activityFlag = false

  def activity = activityFlag

  def activity_=(stat: Boolean) = {
    stat match {
    // Need to get started
      case true if !this.activityFlag => {
        this.activityFlag = true
        spawnActivity
      }
      // Already running
      case true if this.activityFlag =>
      // Already not running
      case false if !this.activityFlag =>
      // Need to update activity to stop
      case false if this.activityFlag =>
        this.activityFlag = false
    }
    this.activityFlag
  }

  //
  // Main heartbeat collection
  //
  protected val m = new Mongo
  protected val db = m getDB DEFAULT_DB
  protected val heartbeat = db getCollection HEARTBEAT_COLLECTION+ "_" + pool
  protected val sharedPool = db getCollection POOL_COLLECTION + "_" + pool
  protected val instance = db getCollection uuid.toString

  def authenticate(user:String,pswd:String) = db.authenticate(user,pswd.toCharArray)

  protected val ID = new BasicDBObject
  ID.put("_id", uuid.toString)
  protected val createdAt = System.currentTimeMillis

  // The shutdown hook for this instance
  private val sdh = new SnareShutdownHook(this)

  protected def registerToPool = {
    val nc = networkConfiguration
    nc.put("ts", System.currentTimeMillis)
    val entry = new BasicDBObject
    entry.put("_id",uuid.toString)
    entry.put("ts",createdAt)
    entry.put("name",name)
    entry.put("pool",pool)
    entry.put("interfaces",nc)
    sharedPool.update(ID, entry, true, false)
    Runtime.getRuntime.addShutdownHook(sdh)
    this
  }

  protected def unregisterFromPool = {
   try {
      heartbeat.remove(ID)
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
  protected def spawnActivity = {
    //
    // The heart beat
    //
    spawn {
      try {
        registerToPool
        log info "[HRTB] Heartbeat engaged for " + uuid
        while (activityFlag) {
          try {
            Thread.sleep(HEARTBEAT_INTERVAL)
            val update = new BasicDBObject
            val value = new BasicDBObject
            value.put("ts", System.currentTimeMillis)
            update.put("$set", value)

            // Activity update
            heartbeat.update(ucnd, update, true, false)
            //log finest "[HRTB] Heartbeat for " + uuid + " " + update
          }
          catch {
            case e => log warning "[FAIL] Heartbeat on " + uuid + " " + e.getCause
          }
        }
      }
      catch {
        case e => {
          log warning "[FAIL] Heartbeat failed to register " + uuid + " " + e.getCause
          activityFlag = false
        }
      }
      try {
        unregisterFromPool
        log info "[HRTB] Heartbeat disengaged for " + uuid
      }
      catch {
        case e => {
          log warning "[FAIL] Heartbeat failed to unregister " + uuid + " " + e.getCause
          activityFlag = false
        }
      }
      this
    }
    //
    // The notification threat
    //
    spawn {
      log info "[EVTL] Notification event loop engaged for " + uuid
      while (activityFlag) {
        try {
          Thread.sleep(EVENT_LOOP_INTERVAL)
          val cur = instance.find
          // log finest "[EVTL] Notifications available " + cur.hasNext
          while (cur.hasNext) {
            try {
              val msg = cur.next.asInstanceOf[BasicDBObject]
              if (notify(msg))
                log info "[EVTL] Notification processed by " + uuid + " " + msg
              else
                log info "[EVTL] Notification ignored by " + uuid + " " + msg
              instance.remove(msg)
            }
            catch {
              case e => log warning "[EVTL] Exeception while processing notification on " + uuid + " " + e.toString
            }
          }
        }
        catch {
          case e => log warning "[EVTL] Exception on notification event loop on " + uuid + " " + e.toString
        }
      }
      log info "[EVTL] Notification event loop disengaged for " + uuid
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

  def fetchPeerInformation (uuid: String) = {
      val peer = new BasicDBObject
      peer.put("_id", uuid)
      if (sharedPool.find(peer).count == 1)
        Some(sharedPool.findOne(peer))
      else
        None
  }

  def fetchRegisteredPeersInformation = {
    var res: List[BasicDBObject] = Nil
    val peer = new BasicDBObject
    peer.put("_id", uuid.toString)
    val cur = sharedPool.find()
    while (cur.hasNext) res ::= cur.next.asInstanceOf[BasicDBObject]
    res
  }

  override def toString = "<Snare: "+name+", "+pool+", "+host+", "+port+", activity="+activityFlag+">"

  private class SnareShutdownHook(snare: Snare) extends Thread {
    val log = LoggerFactory.getLogger

    override def run() {
      log severe "Abnormal finalization. Cleaning after " + snare.uuid + ":" + snare.name
      snare.unregisterFromPool
      log severe "Broadcasting abnormal termination of " + snare.uuid + ":" + snare.name
      snare.broadcast("""{"msg":"killed","type":"fatal","uuid":""" +
              '"' + snare.uuid + '"' + ""","ts":""" + System.currentTimeMillis + "}")
    }
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

  val version = "0.3vcli"
  
  def apply(name: String, pool: String, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, "localhost", 27017, notify)

  def apply(name: String, pool: String, port: Int, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, "localhost", port, notify)

  def apply(name: String, pool: String, host: String, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, host, 27017, notify)

  def apply(name: String, pool: String, host: String, port: Int, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, host, port, notify)

  def unapply(s: Snare): Option[(UUID,String, String, String, Int, Boolean)] = Some((s.uuid, s.name, s.pool, s.host, s.port, s.activityFlag))
}