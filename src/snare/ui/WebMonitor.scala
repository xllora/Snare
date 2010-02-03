package snare.ui

import crochet.Crochet
import snare.logger.LoggerFactory
import org.joda.time.{Period, DateTime}
import snare.Snare
import snare.storage.QueryTools
import org.joda.time.format.DateTimeFormat
import com.mongodb.{BasicDBList, BasicDBObject, Mongo}

/**
 * A basic web monitor class of a pool of Snare monitors.
 *
 * @author Xavier Llora
 * @date Feb 2, 2010 at 1:12:29 PM
 *
 */

class WebMonitor(val pool: String, val host: String, val port: Int) extends Crochet {
  protected val log = LoggerFactory.log
  val DEFAULT_DB = "Snare"
  val HEARTBEAT_COLLECTION = "heartbeat"
  val POOL_COLLECTION = "pool"

  val fmt = DateTimeFormat.forPattern("HH:mm:ss.SSS (z). MMMM dd, yyyy");
  val fmtSmall = DateTimeFormat.forPattern("yyyy-M-dd, HH:mm:ss.SSS(z)");

  //
  // Main store interface object
  //
  val storage = QueryTools(pool,host,port,log)


  //
  // Authentication
  //
  def authenticate(user: String, pswd: String) = storage.authenticate(user, pswd)

  //
  // Basic web interface
  //

  def htmlHeader(title: String) =
    <head>
      <title>
        {title}
      </title>
      <script type="text/javascript" src="http://vis.stanford.edu/protovis/protovis-r3.1.0.js"></script>
      <style type="text/css">
        {"""
          body {
              color: #444;
              background: white;
              font-family: Helvetica,Verdana;
          }
          table {
              border: 1px solid #CCC;
              border-collapse:collapse;
              width: 100%;
          }
          th, td {
              border: 1px solid #CCC;
              border-collapse:collapse;
              padding-left:20px;
              padding-right:20px;
          }
          th {
              background: #888;
              font-size:13px;                            
              color:#FFF;
          }
          td {
              font-family: Courier;
              font-size:11px;
              text-align:center;
          }
          td.red {
            color: white;
            background:red;
          }
          td.orange {
            color: white;
            background:orange;
          }
          td.green {
            color: white;
            background: green;
          }
          td.left {
            text-align:left;
          }
          td.right {
            text-align:right;
          }
          th.left {
            text-align:left;
          }
          th.right {
            text-align:right;
          }
          div.flushleft {
            font-size: 10px;
            float:left;
          }
          div.flushright {
            font-size: 10px;
            float:right;
          }
        """}
      </style>
    </head>


  get("/snare/"+pool+"/heartbeat") {
    val    hb = storage.queryHeartbeats
    val   now = new DateTime
    var total = 0
    if ( header("Accept").indexOf("application/json")>=0 ) {
      val res = new BasicDBList
      hb match {
         case Some(h) if h.length>0 => h.map(res add _)
         case _ =>
      }
      res
    }
    else {
      <html>
        {htmlHeader("Heartbeat status for pool "+pool)}
        <body>
          <div>
            <table>
              <tr><th>ID</th><th>Heartbeat Skew</th><th>Registered on</th></tr>
              {
                val bound = Snare.HEARTBEAT_INTERVAL + Snare.HEARTBEAT_INTERVAL/10
                hb match {
                  case Some(h) if h.length>0 => h.map(
                                  o => { total+=1 ;
                                     <tr><td><a href={"/snare/"+pool+"/"+(o getString "_id")}>{o getString "_id"}</a></td>
                                                                  <td class={if ((now.getMillis-o.getLong("ts"))>bound) "red" else if ((now.getMillis-o.getLong("ts"))<0) "orange" else "green"}>{now.getMillis-o.getLong("ts")}</td>
                                     <td>{fmtSmall print new DateTime(o.getLong("createdAt"))}</td></tr> })
                  case None => <tr><td class="red" colspan="3">Failed to connect to the server</td></tr>
                  case _ => ""
                }
              }
              </table>
              <div class="flushleft">{"Snare "+Snare.version+".     Total number or heartbeats = "+total+"."}</div><div class="flushright">{fmt print new DateTime}</div>
          </div>
        </body>
      </html>
    }
  }

  get("/snare/"+pool+"/info") {
    var total = 0
    val peersInfo = storage.queryFetchRegisteredPeersInformation
    if ( header("Accept").indexOf("application/json")>=0 ) {
      val res = new BasicDBList
      peersInfo match {
         case Some(p) if p.length>0 => p.map(res add _)
         case _ =>
      }
      res
    }
    else {
      <html>
        {htmlHeader("Information for pool "+pool)}
        <body>
          <div>
            <table>
              <tr><th>ID</th><th>Pending</th><th>Registered</th><th>Pool/Name</th><th>Metadata</th></tr>
              {
                peersInfo match {
                  case Some(p) if p.length>0 => p.map (
                          o => { total+=1
                               <tr>
                               <td><a href={"/snare/"+pool+"/"+(o getString "_id")}>{o getString "_id"}</a></td>
                               <td>{storage.queryPendingNotifications(o getString "_id").getOrElse("Unknown")}</td>
                               <td>{fmtSmall print new DateTime(o getLong "ts")}</td>
                               <td>{o getString "pool"}/{o getString "name"}</td>
                               <td><pre>{o getString "metadata"}</pre></td>
                               </tr>})
                  case None => <tr><td class="red" colspan="3">{"Failed to connect to the server"}</td></tr>
                  case _ => ""
                }
              }
             </table>
             <div class="flushleft">{"Snare "+Snare.version+".     Total number or registerd Snares = "+total+"."}</div><div class="flushright">{fmt print new DateTime}</div>
          </div>
        </body>
      </html>
    }
  }

  
  get(("^/snare/"+pool+"""/([a-f0-9\-]+)$""").r) {
    val uuid = elements(0)
    val sinf = storage.queryFetchPeerInformation(uuid)
    if ( header("Accept").indexOf("application/json")>=0 ) {
      sinf match {
         case Some(p) => p
         case _ => new BasicDBObject
      }
    }
    else {
      <html>
        {htmlHeader("Information for peer "+uuid+" in pool "+pool)}
        <body>
          <div>
            <table>
            {
               sinf match {
                case Some(o) =>
                      <tr><th class="right">ID</th><td class="left">{o getString "_id"}</td></tr>
                      <tr><th class="right">Registered on</th><td class="left">{fmt print new DateTime(o.getLong("ts"))}</td></tr>
                      <tr><th class="right">Name</th><td class="left">{o getString "name"}</td></tr>
                      <tr><th class="right">Pool</th><td class="left">{o getString "pool"}</td></tr>
                      <tr><th class="right">Pending notifications</th><td class="left">{storage.queryPendingNotifications(o getString "_id").getOrElse("Unknown")}</td></tr>
                      <tr><th class="right">Metadata</th><td class="left"><pre>{o getString "metadata"}</pre></td></tr>
                      <tr><th class="right">Host</th><td class="left">{o.get("interfaces").asInstanceOf[BasicDBObject].getString("hostname")}</td></tr>
                      <tr><th class="right">IP</th><td class="left">{o.get("interfaces").asInstanceOf[BasicDBObject].getString("ip")}</td></tr>
                      <tr><th class="right">Interfaces</th><td class="left"><pre>{o.get("interfaces").asInstanceOf[BasicDBObject].getString("networks").toString.replaceAll("""\},""","},\n")}</pre></td></tr>

                case None =>  <tr><td class="red" colspan="3">{"Peer "+uuid+" not found"}</td></tr>
              }
             }
             </table>
            <div class="flushleft">{"Snare "+Snare.version+"."}</div><div class="flushright">{fmt print new DateTime}</div>
          </div>
        </body>
      </html>
    }
  }

}

/**
 * WebMonitor companion object
 *
 * @author Xavier Llora
 * @date Feb 2, 2010 at 1:32:29 PM
 *
 */
object WebMonitor {
  def apply(pool: String) = new WebMonitor(pool, "localhost", 27017)

  def apply(pool: String, port: Int) = new WebMonitor(pool, "localhost", port)

  def apply(pool: String, host: String) = new WebMonitor(pool, host, 27017)

  def apply(pool: String, host: String, port: Int) = new WebMonitor(pool, host, port)

  def unapply(wm: WebMonitor): Option[(String, String, Int)] = Some((wm.pool, wm.host, wm.port))
}