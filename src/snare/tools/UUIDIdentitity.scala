package snare.tools

import java.net.{InetAddress,NetworkInterface}
import java.util.UUID
import com.mongodb.{BasicDBList, BasicDBObject}

/**
 * This object allows to compute a unique and repeatable UUID
 * for a given machine
 *
 * @author Xavier Llora
 * @date Jan 27, 2010 at 11:05:14 AM
 *
 */

object UUIDIdentitity {
  def uniqueUUID(uniqueToken: String) = {
    val nic = NetworkInterface.getNetworkInterfaces
    var sb = new StringBuffer(uniqueToken)
    while (nic.hasMoreElements) {
      val card = nic.nextElement.getInetAddresses
      while (card.hasMoreElements)
        sb append (card.nextElement.toString)
    }
    UUID nameUUIDFromBytes sb.toString.getBytes
  }

  def networkConfiguration = {
    val nicList = new BasicDBList()
    val nic = NetworkInterface.getNetworkInterfaces
    while (nic.hasMoreElements) {
      val res = new BasicDBObject
      val card = nic.nextElement.getInetAddresses
      while (card.hasMoreElements) {
        val ip = card.nextElement
        res.put("hostname", ip.getCanonicalHostName)
        res.put("ip", ip.getHostAddress)
      }
      nicList add res
    }
    val res  = new BasicDBObject
    val inet = InetAddress.getLocalHost
    res.put("hostname",inet.getCanonicalHostName)
    res.put("ip",inet.getHostAddress)
    res.put("networks",nicList)
    res
  }

}