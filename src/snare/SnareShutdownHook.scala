package snare

import logger.LoggerFactory

/**
 * Ths implements a shutdown hook linked to an Snare instance in an effort to
 * clean the available data
 *
 * @author Xavier Llora
 * @date Jan 27, 2010 at 2:53:56 PM
 * 
 */

class SnareShutdownHook(snare:Snare) extends Thread {

  val log = LoggerFactory.getLogger

  override def run () {
    log warning "Abnormal finalization. Cleaning after "+snare.uuid+":"+snare.name
    snare.unregisterFromPool
    //TODO Should I broadcast the failure?
  }
}