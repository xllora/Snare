package snare

import logger.LoggerFactory
import scala.concurrent.ops.spawn
import com.mongodb.{BasicDBObject, Mongo}
import snare.tools.UUIDIdentitity._
import java.util.UUID
import snare.tools.Implicits._
import snare.storage.QueryTools._

/**
 * The Snare main class
 *
 * @author Xavier Llora
 * @date Jan 25, 2010 at 7:30:48 PM
 *
 */

class Snare(val name: String, val pool: String, val metadata:BasicDBObject,
            val host: String, val port: Int,
            notify: (BasicDBObject) => Boolean) {
  protected val log = LoggerFactory.log
  val DEFAULT_DB = "Snare"
  val HEARTBEAT_COLLECTION = "heartbeat"
  val POOL_COLLECTION = "pool"
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
  // Main collections
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
    entry.put("metadata",metadata)
    entry.put("interfaces",nc)
    sharedPool.update(ID, entry, true, false)
    updateHeartbeat(createdAt)
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

  protected def updateHeartbeat = {
    val update = new BasicDBObject
    val value = new BasicDBObject
    value.put("ts", System.currentTimeMillis)
    update.put("$set", value)

    // Activity update
    heartbeat.update(ucnd, update, true, false)
  }

  protected def updateHeartbeat(createTS:Long) = {
    val update = new BasicDBObject
    val value = new BasicDBObject
    value.put("createdAt",createTS)
    value.put("ts", System.currentTimeMillis)
    update.put("$set", value)

    // Activity update
    heartbeat.update(ucnd, update, true, false)
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
            Thread.sleep(Snare.HEARTBEAT_INTERVAL)
            updateHeartbeat
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
          Thread.sleep(Snare.EVENT_LOOP_INTERVAL)
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
  def peers = queryPeers(sharedPool,host,port,pool,log)

  def broadcast(message: BasicDBObject) = queryBroadcast(message,sharedPool, db, uuid, host, port, pool, log)

  def notifyPeer(uuid: String, message: BasicDBObject) = queryNotifyPeer(uuid, message, db, sharedPool, host, port, pool, log)

  def fetchPeerInformation (uuid:String) = queryFetchPeerInformation(uuid, sharedPool, host, port, pool, log)

  def fetchRegisteredPeersInformation =  queryFetchRegisteredPeersInformation(sharedPool,host,port,pool,log)

  override def toString = "<Snare: "+name+", "+pool+", "+host+", "+port+", activity="+activityFlag+">"

  private class SnareShutdownHook(snare: Snare) extends Thread {
    val log = LoggerFactory.log

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

  val version = "0.4vcli"

  var HEARTBEAT_INTERVAL = 3000
  var EVENT_LOOP_INTERVAL = 6000

  def apply(name: String, pool: String, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, new BasicDBObject, "localhost", 27017, notify)

  def apply(name: String, pool: String, metadata: BasicDBObject, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, metadata, "localhost", 27017, notify)

  def apply(name: String, pool: String, metadata: BasicDBObject, port: Int, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, metadata, "localhost", port, notify)

  def apply(name: String, pool: String, metadata: BasicDBObject, host: String, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, metadata, host, 27017, notify)

  def apply(name: String, pool: String, metadata: BasicDBObject, host: String, port: Int, notify: (BasicDBObject) => Boolean) = new Snare(name, pool, metadata, host, port, notify)

  def unapply(s: Snare): Option[(UUID, String, String, BasicDBObject, String, Int, Boolean)] = Some((s.uuid, s.name, s.pool, s.metadata, s.host, s.port, s.activityFlag))
}