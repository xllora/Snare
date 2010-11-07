package snare.test

import org.specs.Specification
import snare.Snare
import snare.tools.Implicits._
import com.mongodb.BasicDBObject

/**
 * Basic specifications to test the behavior of Snare
 *
 * @author Xavier Llora
 * @date Feb 3, 2010 at 10:15:24 AM
 *
 */
// TODO Add proper tests for the webui interface
object SnareSpecs extends Specification {
  "Snare birds " should {

    val SNARES_FOR_TEST = 9
    val SLEEP_STEP = 1000
    val MAX_ATTEMPTS = 10

    "Register and have hearbeats " in {
      var notifications = 0
      // Create the snares
      val snares = (1 to SNARES_FOR_TEST).toList.map((i) => Snare("X" + i, "my_pool", (o) => {notifications+=1; true}))
      val s = snares(0)

      // Start the monitors
      snares.map(_.activity = true).map( _ must beTrue)

      var peerAttemps = 0
      var peers = 0
      while ( peers!=SNARES_FOR_TEST && peerAttemps<MAX_ATTEMPTS) {
        Thread.sleep(SLEEP_STEP)
        peers = s.peers.getOrElse(List[String]()).length
        peerAttemps += 1
      }
      peers must beEqualTo(SNARES_FOR_TEST)

      s.heartbeats.getOrElse(List[String]()).length must beEqualTo(SNARES_FOR_TEST)

      s.broadcast("""{"msg":"Hello World!"}""")
      val peerUIUDs = s.peers.getOrElse(List[String]())
      peerUIUDs.length must beEqual(SNARES_FOR_TEST)
      peerUIUDs.foreach(s.notifyPeer(_,"""{"msg":"Hello World!"}"""))

      var notificationAttempts = 0
      while ( notifications!=2*SNARES_FOR_TEST && notificationAttempts<2*MAX_ATTEMPTS) {
        Thread.sleep(SLEEP_STEP)
        notificationAttempts += 1
      }
      notifications must beEqualTo(2*SNARES_FOR_TEST)

      snares.foreach(p=>s.fetchPeerInformation(p.uuid.toString) must beSomething)
      s.fetchRegisteredPeersInformation.getOrElse(List[BasicDBObject]()).length must beEqualTo(SNARES_FOR_TEST)
      
      // Stop the monitors
      snares.map(_.activity = false).map( _ must beFalse)
    }

  }

}