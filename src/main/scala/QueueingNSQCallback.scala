import java.util

import com.trendrr.nsq.{NSQMessageCallback, NSQMessage}
import org.slf4j.{LoggerFactory, Logger}

class QueueingNSQCallback(queue: util.Queue[NSQMessage], msgIdMap: util.Map[Array[Byte],NSQMessage], maxAttempts: Int)
  extends NSQMessageCallback {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  override def error(x: Exception): Unit = {
    // fail harder
  }

  override def message(message: NSQMessage): Unit = {
    if(message.getAttempts > maxAttempts) {
      // TODO maybe write failed message to a file
      logger.info("Giving up on " + message.getId.toString)
      message.finished()
    } else {
      msgIdMap.put(message.getId, message)
      queue.add(message)
    }
  }
}
