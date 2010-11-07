package snare.logger

import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging._

/**
 * The basic logger factory to be use in Snare
 *
 * @author Xavier Llora
 * @date Jan 25, 2010 at 9:20:11 PM
 *
 */

object LoggerFactory {
  protected val LOGGER = "snare.logger.LoggerFactory"
  protected var logger = Logger.getLogger(LOGGER)
  protected val frmt = new SnareFormatter
  if (logger.getHandlers.length > 0)
    for (hdl <- logger.getHandlers)
      hdl setFormatter frmt 
  val parent = logger.getParent
  if (parent != null)
    for (hdl <- parent.getHandlers)
      hdl setFormatter frmt

  def addHandler(hdl:Handler) = logger.addHandler(hdl) ; logger

  def log = logger

  def log_= (l:Logger) = logger = l

  class SnareFormatter extends Formatter {
    val MAX_THREAD_NAME_LENGTH = 40
    val NEW_LINE = System.getProperty("line.separator")
    val FORMATER = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS");

    def format(record: LogRecord) = {
      val sTimeStamp = FORMATER.format(new Date(record.getMillis()))
      sTimeStamp + "::" + record.getLevel + ":" + record.getMessage + NEW_LINE
    }
  }
}